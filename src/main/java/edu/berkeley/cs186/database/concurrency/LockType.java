package edu.berkeley.cs186.database.concurrency;

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (a == NL || b == NL) return true;
        if (a == X || b == X) return false;
        if (a == IS) {
            if (b == IS || b == IX || b == S || b == SIX) return true;
            else return false;
        }
        if (a == IX) {
            if (b == IS || b == IX) return true;
            else return false;
        }
        if (a == S) {
            if (b == IS || b == S) return true;
            return false;
        }
        if (a == SIX) {
            if (b == IS) return true;
            return false;
        }

        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }

        if (childLockType == NL) return true;
        // TODO(proj4_part1): implement
        if (childLockType == IS || childLockType == S) {
            return parentLockType != NL && parentLockType != SIX;
        }

        if (childLockType == IX || childLockType == X || childLockType == SIX) {
            return parentLockType == IX || parentLockType == SIX || parentLockType == X;
        }

        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        switch (required) {
            case S: return substitute == X || substitute == SIX || substitute == S;
            case X: return substitute == X;
            case IS: return substitute != NL;
            case IX: return substitute != NL && substitute != IS && substitute != S;
            case SIX: return substitute == SIX || substitute == X;
            case NL: return substitute == NL;
        }

        return false;
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

