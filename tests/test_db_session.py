import app.db.session as session_mod
import pytest
from sqlalchemy.exc import OperationalError


class FakeSession:
    def __init__(self, name: str) -> None:
        self.name = name
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeEngine:
    def __init__(self) -> None:
        self.dispose_calls = 0

    def dispose(self) -> None:
        self.dispose_calls += 1


def test_run_read_query_retries_operational_error(monkeypatch) -> None:
    created = [FakeSession('first'), FakeSession('second')]
    queue = created.copy()
    engine = FakeEngine()

    monkeypatch.setattr(session_mod, 'get_session_factory', lambda: lambda: queue.pop(0))
    monkeypatch.setattr(session_mod, 'get_engine', lambda: engine)
    monkeypatch.setattr(session_mod, 'sleep', lambda _: None)

    seen: list[str] = []

    def operation(session):
        seen.append(session.name)
        if session.name == 'first':
            raise OperationalError('SELECT 1', {}, Exception('closed'))
        return 'ok'

    assert session_mod.run_read_query(operation) == 'ok'
    assert seen == ['first', 'second']
    assert engine.dispose_calls == 1
    assert all(session.closed for session in created)


def test_run_read_query_raises_after_exhausting_retries(monkeypatch) -> None:
    created = [FakeSession('first'), FakeSession('second')]
    queue = created.copy()
    engine = FakeEngine()

    monkeypatch.setattr(session_mod, 'get_session_factory', lambda: lambda: queue.pop(0))
    monkeypatch.setattr(session_mod, 'get_engine', lambda: engine)
    monkeypatch.setattr(session_mod, 'sleep', lambda _: None)

    def operation(_session):
        raise OperationalError('SELECT 1', {}, Exception('closed'))

    with pytest.raises(OperationalError):
        session_mod.run_read_query(operation)

    assert engine.dispose_calls == 2
    assert all(session.closed for session in created)
