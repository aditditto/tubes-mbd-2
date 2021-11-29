// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.
#include <iostream>
#include "txn/lock_manager.h"

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  LockRequest r(EXCLUSIVE, txn);

  if (!lock_table_[key])
  {
    lock_table_[key] = new deque<LockRequest>();
  }

  if (lock_table_[key]->empty())
  {
    lock_table_[key]->push_back(r);
    return true;
  }

  lock_table_[key]->push_back(r);
  txn_waits_[txn]++;

  return false;
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!

  deque<LockRequest> *q = lock_table_[key];

  if (q->front().txn_ == txn)
  {
    q->pop_front();
    if (!q->empty())
    {
      LockRequest active = q->front();

      txn_waits_[active.txn_]--;

      if (txn_waits_[active.txn_] == 0)
      {
        ready_txns_->push_back(active.txn_);
        txn_waits_.erase(active.txn_);
      }
    }
  }
  else
  {
    for (auto it = q->begin(); it < q->end(); it++)
    {
      if (it->txn_ == txn)
      {
        q->erase(it);
        break;
      }
    }
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!
  if (!lock_table_[key])
  {
    return UNLOCKED;
  }

  owners->clear();
  owners->push_back(lock_table_[key]->front().txn_);

  return EXCLUSIVE;
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!
  return UNLOCKED;
}

