from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import dramatiq
from sqlalchemy.dialects.postgresql import insert

from app.connectors.bulk_file import BulkFileConnector
from app.connectors.florida.mapper import build_company_event, build_registry_snapshot
from app.connectors.florida.parser import inspect_source_file, iter_source_records
from app.db.models import (
    CompanyEvent,
    CompanyRegistrySnapshot,
    JobRun,
    JobStatus,
    RawRegistryRecord,
    SourceFile,
    SourceFileStatus,
    SourceRecordParseStatus,
    SourceRecordRef,
)
from app.db.session import get_session_factory
from app.services.object_store import get_object_store
from app.workers.broker import broker  # noqa: F401

FLORIDA_BATCH_SIZE = 1000


def build_connector(state: str) -> BulkFileConnector:
    return BulkFileConnector(state=state)


@dramatiq.actor(max_retries=5)
def import_registry_drop(state: str, source_path: str) -> None:
    if state.upper() == "FL":
        job_run_id, source_file_id = _import_florida_registry_drop(Path(source_path))
    else:
        job_run_id = _import_generic_registry_drop(state, Path(source_path))
        source_file_id = None

    from app.workers.tasks_normalize import normalize_entities

    normalize_entities.send(
        state.upper(),
        str(job_run_id),
        str(source_file_id) if source_file_id else None,
    )


@dramatiq.actor(max_retries=5, queue_name="fl_import")
def import_source_file(source_file_id: str) -> None:
    source_file_uuid = uuid.UUID(source_file_id)
    session = get_session_factory()()
    try:
        source_file = session.get(SourceFile, source_file_uuid)
        if source_file is None or not source_file.bucket_key:
            return
    finally:
        session.close()

    object_store = get_object_store()
    with TemporaryDirectory() as temp_dir:
        source_path = Path(temp_dir) / source_file.filename.split("#", 1)[0]
        object_store.write_to_path(source_file.bucket_key, source_path)
        job_run_id = _import_existing_florida_source_file(source_file_uuid, source_path)

    from app.workers.tasks_normalize import normalize_entities

    normalize_entities.send("FL", str(job_run_id), source_file_id)


def _import_generic_registry_drop(state: str, source_path: Path) -> uuid.UUID:
    connector = build_connector(state)
    batch = connector.load(source_path)
    session = get_session_factory()()
    job_run: JobRun | None = None

    try:
        job_run = JobRun(
            connector_kind=connector.connector_kind,
            state=state.upper(),
            source_uri=batch.source_uri,
            source_checksum=batch.source_checksum,
            status=JobStatus.running,
            stats={},
        )
        session.add(job_run)
        session.flush()

        stage_values = [
            {
                "job_run_id": job_run.id,
                "state": state.upper(),
                "external_filing_id": record.external_filing_id,
                "record_checksum": record.checksum,
                "payload": record.payload,
                "source_metadata": {"connector_kind": connector.connector_kind},
            }
            for record in batch.records
        ]

        if stage_values:
            stmt = insert(RawRegistryRecord).values(stage_values)
            stmt = stmt.on_conflict_do_nothing(index_elements=["job_run_id", "record_checksum"])
            session.execute(stmt)

        job_run.status = JobStatus.completed
        job_run.finished_at = datetime.now(UTC)
        job_run.stats = {
            "record_count": len(batch.records),
            "connector_kind": connector.connector_kind,
        }
        session.add(job_run)
        session.commit()
        return job_run.id
    except Exception:
        session.rollback()
        if job_run is not None:
            job_run.status = JobStatus.failed
            job_run.finished_at = datetime.now(UTC)
            session.add(job_run)
            session.commit()
        raise
    finally:
        session.close()


def _import_florida_registry_drop(source_path: Path) -> tuple[uuid.UUID, uuid.UUID]:
    source_details = inspect_source_file(source_path)
    session = get_session_factory()()
    job_run: JobRun | None = None
    source_file: SourceFile | None = None

    stats = {
        "source_kind": source_details.source_kind.value,
        "record_count": 0,
        "parsed_records": 0,
        "failed_records": 0,
        "snapshot_records": 0,
        "event_records": 0,
        "parser_version": source_details.parser_version,
    }

    try:
        job_run = JobRun(
            connector_kind="florida_source_file",
            state="FL",
            source_uri=source_details.source_uri,
            source_checksum=source_details.source_checksum,
            status=JobStatus.running,
            stats={},
        )
        session.add(job_run)
        session.flush()

        source_file = SourceFile(
            job_run_id=job_run.id,
            provider=source_details.provider,
            source_kind=source_details.source_kind,
            state="FL",
            filename=source_details.filename,
            source_uri=source_details.source_uri,
            bucket_key=source_details.bucket_key,
            source_checksum=source_details.source_checksum,
            record_length=source_details.record_length,
            file_date=source_details.file_date,
            is_delta=source_details.is_delta,
            status=SourceFileStatus.processing,
            metadata_json={
                "archive_members": source_details.archive_members,
                "parser_version": source_details.parser_version,
            },
        )
        session.add(source_file)
        session.flush()

        ref_values: list[dict[str, object]] = []
        snapshot_values: list[dict[str, object]] = []
        event_values: list[dict[str, object]] = []

        for record in iter_source_records(source_path):
            ref_id = uuid.uuid4()
            stats["record_count"] += 1

            ref_value = {
                "id": ref_id,
                "source_file_id": source_file.id,
                "record_no": record.record_no,
                "byte_offset": record.byte_offset,
                "raw_hash": record.raw_hash,
                "external_filing_id": record.external_filing_id,
                "parser_version": source_details.parser_version,
                "parse_status": record.parse_status,
                "error_code": record.error_code,
            }

            if record.parse_status == SourceRecordParseStatus.parsed:
                try:
                    if source_details.source_kind.value.endswith("_events"):
                        event_record = build_company_event(record.payload)
                        event_values.append(
                            {
                                "source_file_id": source_file.id,
                                "source_record_ref_id": ref_id,
                                "state": event_record.state,
                                "external_filing_id": event_record.external_filing_id,
                                "legal_name": event_record.legal_name,
                                "event_code": event_record.event_code,
                                "event_description": event_record.event_description,
                                "effective_date": event_record.effective_date,
                                "filed_date": event_record.filed_date,
                                "payload_json": event_record.payload,
                            },
                        )
                        stats["event_records"] += 1
                    else:
                        snapshot_record = build_registry_snapshot(record.payload)
                        snapshot_values.append(
                            {
                                "source_file_id": source_file.id,
                                "source_record_ref_id": ref_id,
                                "state": snapshot_record.state,
                                "external_filing_id": snapshot_record.external_filing_id,
                                "legal_name": snapshot_record.legal_name,
                                "normalized_name": snapshot_record.normalized_name,
                                "status": snapshot_record.status,
                                "filing_type": snapshot_record.filing_type,
                                "formed_at": snapshot_record.formed_at,
                                "last_transaction_date": snapshot_record.last_transaction_date,
                                "latest_report_year": snapshot_record.latest_report_year,
                                "latest_report_date": snapshot_record.latest_report_date,
                                "fei_number": snapshot_record.fei_number,
                                "principal_address_json": snapshot_record.principal_address,
                                "mailing_address_json": snapshot_record.mailing_address,
                                "registered_agent_json": snapshot_record.registered_agent,
                                "officers_json": snapshot_record.officers,
                                "registry_payload": snapshot_record.registry_payload,
                                "is_current": True,
                            },
                        )
                        stats["snapshot_records"] += 1
                    stats["parsed_records"] += 1
                except ValueError:
                    ref_value["parse_status"] = SourceRecordParseStatus.failed
                    ref_value["error_code"] = "mapping_error"
                    stats["failed_records"] += 1
                else:
                    ref_values.append(ref_value)
                    if len(ref_values) >= FLORIDA_BATCH_SIZE:
                        _flush_florida_batches(session, ref_values, snapshot_values, event_values)
                    continue
            else:
                stats["failed_records"] += 1

            ref_values.append(ref_value)
            if len(ref_values) >= FLORIDA_BATCH_SIZE:
                _flush_florida_batches(session, ref_values, snapshot_values, event_values)

        _flush_florida_batches(session, ref_values, snapshot_values, event_values)

        source_file.status = (
            SourceFileStatus.completed if stats["record_count"] > 0 else SourceFileStatus.noop
        )
        source_file.total_records = stats["record_count"]
        source_file.processed_at = datetime.now(UTC)
        session.add(source_file)

        job_run.status = JobStatus.completed
        job_run.finished_at = datetime.now(UTC)
        job_run.stats = stats
        session.add(job_run)
        session.commit()
        return job_run.id, source_file.id
    except Exception:
        session.rollback()
        if source_file is not None:
            source_file.status = SourceFileStatus.failed
            source_file.processed_at = datetime.now(UTC)
            session.add(source_file)
        if job_run is not None:
            job_run.status = JobStatus.failed
            job_run.finished_at = datetime.now(UTC)
            job_run.stats = stats
            session.add(job_run)
        session.commit()
        raise
    finally:
        session.close()


def _import_existing_florida_source_file(source_file_id: uuid.UUID, source_path: Path) -> uuid.UUID:
    source_details = inspect_source_file(source_path)
    session = get_session_factory()()
    source_file = session.get(SourceFile, source_file_id)
    if source_file is None:
        session.close()
        raise ValueError(f"Source file not found: {source_file_id}")

    job_run: JobRun | None = None
    stats = {
        "source_kind": source_details.source_kind.value,
        "record_count": 0,
        "parsed_records": 0,
        "failed_records": 0,
        "snapshot_records": 0,
        "event_records": 0,
        "parser_version": source_details.parser_version,
    }
    quarterly_shard = source_file.metadata_json.get("quarterly_shard")

    try:
        source_file.status = SourceFileStatus.processing
        session.add(source_file)
        session.flush()

        job_run = JobRun(
            connector_kind="florida_source_file_import",
            state="FL",
            source_uri=source_file.source_uri,
            source_checksum=source_file.source_checksum,
            status=JobStatus.running,
            stats={},
        )
        session.add(job_run)
        session.flush()

        ref_values: list[dict[str, object]] = []
        snapshot_values: list[dict[str, object]] = []
        event_values: list[dict[str, object]] = []

        for record in iter_source_records(source_path, quarterly_shard=quarterly_shard):
            ref_id = uuid.uuid4()
            stats["record_count"] += 1
            ref_value = {
                "id": ref_id,
                "source_file_id": source_file.id,
                "record_no": record.record_no,
                "byte_offset": record.byte_offset,
                "raw_hash": record.raw_hash,
                "external_filing_id": record.external_filing_id,
                "parser_version": source_details.parser_version,
                "parse_status": record.parse_status,
                "error_code": record.error_code,
            }
            if record.parse_status == SourceRecordParseStatus.parsed:
                try:
                    if source_details.source_kind.value.endswith("_events"):
                        event_record = build_company_event(record.payload)
                        event_values.append(
                            {
                                "source_file_id": source_file.id,
                                "source_record_ref_id": ref_id,
                                "state": event_record.state,
                                "external_filing_id": event_record.external_filing_id,
                                "legal_name": event_record.legal_name,
                                "event_code": event_record.event_code,
                                "event_description": event_record.event_description,
                                "effective_date": event_record.effective_date,
                                "filed_date": event_record.filed_date,
                                "payload_json": event_record.payload,
                            },
                        )
                        stats["event_records"] += 1
                    else:
                        snapshot_record = build_registry_snapshot(record.payload)
                        snapshot_values.append(
                            {
                                "source_file_id": source_file.id,
                                "source_record_ref_id": ref_id,
                                "state": snapshot_record.state,
                                "external_filing_id": snapshot_record.external_filing_id,
                                "legal_name": snapshot_record.legal_name,
                                "normalized_name": snapshot_record.normalized_name,
                                "status": snapshot_record.status,
                                "filing_type": snapshot_record.filing_type,
                                "formed_at": snapshot_record.formed_at,
                                "last_transaction_date": snapshot_record.last_transaction_date,
                                "latest_report_year": snapshot_record.latest_report_year,
                                "latest_report_date": snapshot_record.latest_report_date,
                                "fei_number": snapshot_record.fei_number,
                                "principal_address_json": snapshot_record.principal_address,
                                "mailing_address_json": snapshot_record.mailing_address,
                                "registered_agent_json": snapshot_record.registered_agent,
                                "officers_json": snapshot_record.officers,
                                "registry_payload": snapshot_record.registry_payload,
                                "is_current": True,
                            },
                        )
                        stats["snapshot_records"] += 1
                    stats["parsed_records"] += 1
                except ValueError:
                    ref_value["parse_status"] = SourceRecordParseStatus.failed
                    ref_value["error_code"] = "mapping_error"
                    stats["failed_records"] += 1
                else:
                    ref_values.append(ref_value)
                    if len(ref_values) >= FLORIDA_BATCH_SIZE:
                        _flush_florida_batches(session, ref_values, snapshot_values, event_values)
                    continue
            else:
                stats["failed_records"] += 1

            ref_values.append(ref_value)
            if len(ref_values) >= FLORIDA_BATCH_SIZE:
                _flush_florida_batches(session, ref_values, snapshot_values, event_values)

        _flush_florida_batches(session, ref_values, snapshot_values, event_values)

        source_file.status = (
            SourceFileStatus.completed if stats["record_count"] > 0 else SourceFileStatus.noop
        )
        source_file.total_records = stats["record_count"]
        source_file.processed_at = datetime.now(UTC)
        session.add(source_file)

        job_run.status = JobStatus.completed
        job_run.finished_at = datetime.now(UTC)
        job_run.stats = stats
        session.add(job_run)
        session.commit()
        return job_run.id
    except Exception:
        session.rollback()
        source_file.status = SourceFileStatus.failed
        source_file.processed_at = datetime.now(UTC)
        session.add(source_file)
        if job_run is not None:
            job_run.status = JobStatus.failed
            job_run.finished_at = datetime.now(UTC)
            job_run.stats = stats
            session.add(job_run)
        session.commit()
        raise
    finally:
        session.close()


def _flush_florida_batches(
    session,
    ref_values: list[dict[str, object]],
    snapshot_values: list[dict[str, object]],
    event_values: list[dict[str, object]],
) -> None:
    if ref_values:
        stmt = insert(SourceRecordRef).values(ref_values)
        stmt = stmt.on_conflict_do_nothing(index_elements=["source_file_id", "record_no"])
        session.execute(stmt)
        ref_values.clear()

    if snapshot_values:
        stmt = insert(CompanyRegistrySnapshot).values(snapshot_values)
        stmt = stmt.on_conflict_do_nothing(index_elements=["source_record_ref_id"])
        session.execute(stmt)
        snapshot_values.clear()

    if event_values:
        stmt = insert(CompanyEvent).values(event_values)
        stmt = stmt.on_conflict_do_nothing(index_elements=["source_record_ref_id"])
        session.execute(stmt)
        event_values.clear()
