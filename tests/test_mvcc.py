import pytest
from chronos_db.mvcc import MVCCStore, TransactionConflictError


def test_mvcc_snapshot_isolation():
    store = MVCCStore()

    # Txn 1 writes initial balance
    t1 = store.begin_transaction()
    t1.put("account:alice", b"100")
    t1.commit()

    # Txn 2 starts and reads 100
    t2 = store.begin_transaction()
    assert t2.get("account:alice") == b"100"

    # Txn 3 modifies alice to 250 and commits
    t3 = store.begin_transaction()
    t3.put("account:alice", b"250")
    t3.commit()

    # Txn 2 still reads 100 due to snapshot isolation!
    assert t2.get("account:alice") == b"100"

    # Txn 4 sees the committed 250
    t4 = store.begin_transaction()
    assert t4.get("account:alice") == b"250"


def test_mvcc_write_conflict_detection():
    store = MVCCStore()

    t_init = store.begin_transaction()
    t_init.put("seat:12A", b"free")
    t_init.commit()

    # Concurrent txns attempt to reserve the same seat
    t_user_a = store.begin_transaction()
    t_user_b = store.begin_transaction()

    t_user_a.put("seat:12A", b"reserved_by_Alice")
    t_user_b.put("seat:12A", b"reserved_by_Bob")

    # User A commits first
    t_user_a.commit()

    # User B should fail with write conflict
    with pytest.raises(TransactionConflictError):
        t_user_b.commit()


def test_mvcc_transaction_rollback():
    store = MVCCStore()

    t = store.begin_transaction()
    t.put("temp:key", b"value")
    t.rollback()

    # Should not exist
    t_after = store.begin_transaction()
    assert t_after.get("temp:key") is None
