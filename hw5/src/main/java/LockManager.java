import sun.awt.image.ImageWatched;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * The Lock Manager handles lock and unlock requests from transactions. The
 * Lock Manager will maintain a hashtable that is keyed on the name of the
 * table being locked. The Lock Manager will also keep a FIFO queue of requests
 * for locks that cannot be immediately granted.
 */
public class LockManager {
    private DeadlockAvoidanceType deadlockAvoidanceType;
    private HashMap<String, TableLock> tableToTableLock;

    public enum DeadlockAvoidanceType {
        None,
        WaitDie,
        WoundWait
    }

    public enum LockType {
        Shared,
        Exclusive
    }

    public LockManager(DeadlockAvoidanceType type) {
        this.deadlockAvoidanceType = type;
        this.tableToTableLock = new HashMap<String, TableLock>();
    }

    /**
     * The acquire method will grant the lock if it is compatible. If the lock
     * is not compatible, then the request will be placed on the requesters
     * queue. Once you have implemented deadlock avoidance algorithms, you
     * should instead check the deadlock avoidance type and call the
     * appropriate function that you will complete in part 2.
     * @param transaction that is requesting the lock
     * @param tableName of requested table
     * @param lockType of requested lock
     */
    public void acquire(Transaction transaction, String tableName, LockType lockType)
            throws IllegalArgumentException {
      if (deadlockAvoidanceType == DeadlockAvoidanceType.WoundWait) {
        woundWait(tableName, transaction, lockType);
      } else if (deadlockAvoidanceType == DeadlockAvoidanceType.WaitDie) {
        waitDie(tableName, transaction, lockType);
      } else {
        if (transaction.getStatus() == Transaction.Status.Waiting) {
          throw new IllegalArgumentException();
        }
        TableLock requestedLock = new TableLock(lockType);
        if (!tableToTableLock.isEmpty() && tableToTableLock.containsKey(tableName)) {
          requestedLock = tableToTableLock.get(tableName);
        } else {
          tableToTableLock.put(tableName, requestedLock);
        }
        if (requestedLock.lockOwners.contains(transaction) && requestedLock.lockType == LockType.Exclusive &&
                lockType == LockType.Shared) {
          throw new IllegalArgumentException();
        }
        if (requestedLock.lockOwners.contains(transaction) && requestedLock.lockType == lockType) {
          throw new IllegalArgumentException();
        }
        if (requestedLock.lockOwners.size() == 0) {
          requestedLock.lockOwners.add(transaction);
        } else {
          if (requestedLock.lockOwners.contains(transaction) && requestedLock.lockType == LockType.Shared &&
                  lockType == LockType.Exclusive && requestedLock.lockOwners.size() == 1) {
            requestedLock.lockType = LockType.Exclusive;
          } else if (requestedLock.lockType == LockType.Exclusive) {
            transaction.sleep();
            requestedLock.requestersQueue.add(new Request(transaction, lockType));
          } else if (requestedLock.lockType == LockType.Shared && lockType == LockType.Shared) {
            requestedLock.lockOwners.add(transaction);
          } else if (requestedLock.lockType == LockType.Shared && lockType == LockType.Exclusive) {
            transaction.sleep();
            requestedLock.requestersQueue.addFirst(new Request(transaction, lockType));
          }
        }
      }
    }

    /**
     * This method will return true if the requested lock is compatible. See
     * spec provides compatibility conditions.
     * @param tableName of requested table
     * @param transaction requesting the lock
     * @param lockType of the requested lock
     * @return true if the lock being requested does not cause a conflict
     */
    private boolean compatible(String tableName, Transaction transaction, LockType lockType) {
      TableLock requestedLock = new TableLock(lockType);
        if (requestedLock.lockOwners.size() > 0) {
          if (requestedLock.lockType == LockType.Exclusive) {
            return false;
          }
          if (requestedLock.lockType == LockType.Shared && lockType == LockType.Exclusive) {
            return false;
          }
        }
      return true;
    }

    /**
     * Will release the lock and grant all mutually compatible transactions at
     * the head of the FIFO queue. See spec for more details.
     * @param transaction releasing lock
     * @param tableName of table being released
     */
    public void release(Transaction transaction, String tableName) throws IllegalArgumentException {
      if (transaction.getStatus() == Transaction.Status.Waiting) {
        throw new IllegalArgumentException();
      }
      TableLock releaseLock = tableToTableLock.get(tableName);
      if (releaseLock == null) {
        throw new IllegalArgumentException();
      }
      if (!releaseLock.lockOwners.contains(transaction)) {
        throw new IllegalArgumentException();
      }
      boolean hasRequesters = false;
      if (releaseLock.requestersQueue.size() > 0) {
        hasRequesters = true;
      }
      releaseLock.lockOwners.remove(transaction);
      if (releaseLock.lockOwners.size() == 1) {
        if (releaseLock.lockType == LockType.Shared) {
          if (hasRequesters) {
            if (releaseLock.requestersQueue.get(0).lockType == LockType.Exclusive) {
              releaseLock.lockType = LockType.Exclusive;
              releaseLock.requestersQueue.get(0).transaction.wake();
              releaseLock.requestersQueue.removeFirst();
            }
          }
        }
      } else if (releaseLock.lockOwners.size() > 1 && releaseLock.lockType == LockType.Shared) {
        releaseLock.lockType = LockType.Shared;
        LinkedList<Request> toBeRemoved = new LinkedList<>();
        for (Request r: releaseLock.requestersQueue) {
          if (r.lockType == LockType.Shared) {
            r.transaction.wake();
            releaseLock.lockOwners.add(r.transaction);
            toBeRemoved.add(r);
          }
        }
        for (int i = 0; i < toBeRemoved.size(); i++) {
          releaseLock.requestersQueue.remove(toBeRemoved.get(i));
        }
      } else if (releaseLock.lockOwners.size() == 0) {
        if (hasRequesters) {
          if (releaseLock.requestersQueue.getFirst().lockType == LockType.Shared) {
            releaseLock.lockType = LockType.Shared;
            LinkedList<Request> toBeRemoved = new LinkedList<>();
            for (Request r: releaseLock.requestersQueue) {
              if (r.lockType == LockType.Shared) {
                r.transaction.wake();
                releaseLock.lockOwners.add(r.transaction);
                toBeRemoved.add(r);
              }
            }
            for (int i = 0; i < toBeRemoved.size(); i++) {
              releaseLock.requestersQueue.remove(toBeRemoved.get(i));
            }
          } else if (releaseLock.requestersQueue.getFirst().lockType == LockType.Exclusive) {
            releaseLock.lockType = LockType.Exclusive;
            releaseLock.lockOwners.add(releaseLock.requestersQueue.getFirst().transaction);
            releaseLock.requestersQueue.getFirst().transaction.wake();
            releaseLock.requestersQueue.removeFirst();
          } else {
            tableToTableLock.remove(releaseLock);
          }
        }
      }
    }

    /**
     * Will return true if the specified transaction holds a lock of type
     * lockType on the table tableName.
     * @param transaction holding lock
     * @param tableName of locked table
     * @param lockType of lock
     * @return true if the transaction holds lock
     */
    public boolean holds(Transaction transaction, String tableName, LockType lockType) {
      if (!tableToTableLock.isEmpty()) {
        if (!tableToTableLock.containsKey(tableName)) {
          return false;
        }
        TableLock requestedLock = tableToTableLock.get(tableName);
        if (requestedLock.lockOwners.contains(transaction) && requestedLock.lockType == lockType) {
          return true;
        }
      }
      return false;
    }

    /**
     * If transaction t1 requests an incompatible lock, t1 will abort if it has
     * a lower priority (higher timestamp) than all conflicting transactions.
     * If t1 has a higher priority, it will wait on the requesters queue.
     * @param tableName of locked table
     * @param transaction requesting lock
     * @param lockType of request
     */
    private void waitDie(String tableName, Transaction transaction, LockType lockType) {
      boolean conflict = false;
      if (transaction.getStatus() == Transaction.Status.Waiting) {
        throw new IllegalArgumentException();
      }
      TableLock requestedLock = new TableLock(lockType);
      if (!tableToTableLock.isEmpty() && tableToTableLock.containsKey(tableName)) {
        requestedLock = tableToTableLock.get(tableName);
      } else {
        tableToTableLock.put(tableName, requestedLock);
      }
      if (requestedLock.lockOwners.contains(transaction) && requestedLock.lockType == LockType.Exclusive &&
              lockType == LockType.Shared) {
        throw new IllegalArgumentException();
      }
      if (requestedLock.lockOwners.contains(transaction) && requestedLock.lockType == lockType) {
        throw new IllegalArgumentException();
      }
      if (requestedLock.lockOwners.size() == 0) {
        requestedLock.lockOwners.add(transaction);
      } else {
        if (requestedLock.lockOwners.contains(transaction) && requestedLock.lockType == LockType.Shared &&
                lockType == LockType.Exclusive && requestedLock.lockOwners.size() == 1) {
          requestedLock.lockType = LockType.Exclusive;
        } else if (requestedLock.lockType == LockType.Exclusive) {
          conflict = true;
        } else if (requestedLock.lockType == LockType.Shared && lockType == LockType.Shared){
          requestedLock.lockOwners.add(transaction);
        } else if (requestedLock.lockType == LockType.Shared && lockType == LockType.Exclusive) {
          conflict = true;
        }
      }
      if (conflict) {
        int t1TimeStamp = transaction.getTimestamp();
        boolean existsLowerPriority = false;
        for (Transaction t : requestedLock.lockOwners) {
          if (t.getTimestamp() > t1TimeStamp) {
            existsLowerPriority = true;
          }
        }
        if (!existsLowerPriority) {
          transaction.abort();
        } else {
          requestedLock.requestersQueue.add(new Request(transaction, lockType));
          transaction.sleep();
        }
      }
    }

    /**
     * If transaction t1 requests an incompatible lock, t1 will wait if it has
     * a lower priority (higher timestamp) than conflicting transactions. If t1
     * has a higher priority than every conflicting transaction, it will abort
     * all the lock holders and acquire the lock.
     * @param tableName of locked table
     * @param transaction requesting lock
     * @param lockType of request
     */
    private void woundWait(String tableName, Transaction transaction, LockType lockType) {
      boolean conflict = false;
      if (transaction.getStatus() == Transaction.Status.Waiting) {
        throw new IllegalArgumentException();
      }
      TableLock requestedLock = new TableLock(lockType);
      if (!tableToTableLock.isEmpty() && tableToTableLock.containsKey(tableName)) {
        requestedLock = tableToTableLock.get(tableName);
      } else {
        tableToTableLock.put(tableName, requestedLock);
      }
      if (requestedLock.lockOwners.contains(transaction) && requestedLock.lockType == LockType.Exclusive &&
              lockType == LockType.Shared) {
        throw new IllegalArgumentException();
      }
      if (requestedLock.lockOwners.contains(transaction) && requestedLock.lockType == lockType) {
        throw new IllegalArgumentException();
      }
      if (requestedLock.lockOwners.size() == 0) {
        requestedLock.lockOwners.add(transaction);
      } else {
        if (requestedLock.lockOwners.contains(transaction) && requestedLock.lockType == LockType.Shared &&
                lockType == LockType.Exclusive && requestedLock.lockOwners.size() == 1) {
          requestedLock.lockType = LockType.Exclusive;
        } else if (requestedLock.lockType == LockType.Exclusive) {
          conflict = true;
        } else if (requestedLock.lockType == LockType.Shared && lockType == LockType.Shared){
          requestedLock.lockOwners.add(transaction);
        } else if (requestedLock.lockType == LockType.Shared && lockType == LockType.Exclusive) {
          conflict = true;
        }
      }
      if (conflict) {
        int t1TimeStamp = transaction.getTimestamp();
        int numLowerPriorities = 0;
        for (Transaction t : requestedLock.lockOwners) {
          if (t1TimeStamp < t.getTimestamp()) {
            numLowerPriorities++;
          }
        }
        if (numLowerPriorities == requestedLock.lockOwners.size()) {
          LinkedList<Transaction> xactToRemove = new LinkedList<>();
          LinkedList<Request> reqToRemove = new LinkedList<>();
          for (Transaction t: requestedLock.lockOwners) {
            for (Request r: requestedLock.requestersQueue) {
              if (r.transaction == t){
                reqToRemove.add(r);
              }
            }
            xactToRemove.add(t);
            t.abort();
          }
          for (int i = 0; i < xactToRemove.size(); i++) {
            requestedLock.lockOwners.remove(xactToRemove.get(i));
          }
          for (int i = 0; i < reqToRemove.size(); i++) {
            requestedLock.requestersQueue.remove(reqToRemove.get(i));
          }
          requestedLock.lockType = lockType;
          requestedLock.lockOwners.add(transaction);
          transaction.wake();
        } else {
          requestedLock.requestersQueue.add(new Request(transaction, lockType));
          transaction.sleep();
        }
      }
    }

    /**
     * Contains all information about the lock for a specific table. This
     * information includes lock type, lock owner(s), and lock requestor(s).
     */
    private class TableLock {
        private LockType lockType;
        private HashSet<Transaction> lockOwners;
        private LinkedList<Request> requestersQueue;

        public TableLock(LockType lockType) {
            this.lockType = lockType;
            this.lockOwners = new HashSet<Transaction>();
            this.requestersQueue = new LinkedList<Request>();
        }

    }

    /**
     * Used to create request objects containing the transaction and lock type.
     * These objects will be added to the requestor queue for a specific table
     * lock.
     */
    private class Request {
        private Transaction transaction;
        private LockType lockType;

        public Request(Transaction transaction, LockType lockType) {
            this.transaction = transaction;
            this.lockType = lockType;
        }
    }
}
