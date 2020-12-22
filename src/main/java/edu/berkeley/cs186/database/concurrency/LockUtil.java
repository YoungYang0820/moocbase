package edu.berkeley.cs186.database.concurrency;

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null | lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        if (LockType.substitutable(effectiveLockType, requestType)) return;
        if (LockType.substitutable(requestType, effectiveLockType)) {
            if (explicitLockType == LockType.IS || explicitLockType == LockType.IS || explicitLockType == LockType.SIX)
                escalateLock(lockContext, requestType, transaction);
            else promoteLock(lockContext, requestType, transaction);
            return;
        }
        if (explicitLockType == LockType.IX && requestType == LockType.S) {
            List<ResourceName> names = new ArrayList<>();
            names.add(lockContext.name);
            for (LockContext childContext : lockContext.children.values()) {
                if (childContext.getExplicitLockType(transaction) != LockType.X && childContext.getExplicitLockType(transaction) != LockType.NL) {
                    names.add(childContext.name);
                    lockContext.numChildLocks.put(transaction.getTransNum(), lockContext.numChildLocks.get(transaction.getTransNum()) - 1);
                }
            }
            lockContext.lockman.acquireAndRelease(transaction, lockContext.name, LockType.SIX, names);
            return;
        }
        acquireLock(lockContext, requestType, transaction);

        return;
    }

    private static void acquireLock(LockContext lockContext, LockType requestType, TransactionContext transaction) {
        LockContext parentContext = lockContext.parentContext();
        if (parentContext != null) {
            LockType parentType = parentContext.getExplicitLockType(transaction);
            if (parentType != LockType.parentLock(requestType) && !(parentType == LockType.IX && requestType == LockType.S)) {
                if (parentType != LockType.NL) promoteLock(parentContext, LockType.parentLock(requestType), transaction);
                else acquireLock(parentContext, LockType.parentLock(requestType), transaction);
            }
        }
        lockContext.acquire(transaction, requestType);
    }

    private static void escalateLock(LockContext lockContext, LockType requestType, TransactionContext transaction) {
        List<ResourceName> names = new ArrayList<>();
        names.add(lockContext.name);
        lockContext.escalate(transaction);
    }

    private static void promoteLock(LockContext lockContext, LockType requestType, TransactionContext transaction) {
        LockContext parentContext = lockContext.parentContext();
        if (parentContext != null) promoteLock(parentContext, LockType.parentLock(requestType), transaction);
        lockContext.promote(transaction, requestType);
    }
}
