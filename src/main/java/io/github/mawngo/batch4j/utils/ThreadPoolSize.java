package io.github.mawngo.batch4j.utils;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Contains thread pool configuration, for create {@link ExecutorService} using {@link ExecutorUtils}
 *
 * @see #parse(String)
 */
public final class ThreadPoolSize {
    private final int corePoolSize;
    private final int maximumPoolSize;

    private ThreadPoolSize(int corePoolSize, int maximumPoolSize) {
        this.corePoolSize = corePoolSize;
        this.maximumPoolSize = maximumPoolSize;
    }

    /**
     * @return true if the thread pool is dynamic pool
     * @see ExecutorUtils#newDynamicThreadPool(int, int)
     */
    public boolean isDynamic() {
        return maximumPoolSize > corePoolSize && corePoolSize >= 0;
    }

    /**
     * @return true if the thread pool is single
     * @see Executors#newSingleThreadExecutor()
     */
    public boolean isSingle() {
        return corePoolSize == 1 && maximumPoolSize == 1;
    }

    /**
     * Disabled thread pool config will create a {@code null} {@link ExecutorService}
     *
     * @return true if the thread pool is disabled
     */
    public boolean isDisabled() {
        return corePoolSize == 0 && maximumPoolSize == 0;
    }

    /**
     * @return true if the thread pool is cached
     * @see Executors#newCachedThreadPool()
     */
    public boolean isCached() {
        return corePoolSize == -1 && maximumPoolSize == -1;
    }

    /**
     * @return return true if the thread pool is fixed
     * @see Executors#newFixedThreadPool(int)
     */
    public boolean isFixed() {
        return corePoolSize == maximumPoolSize && corePoolSize > 1;
    }

    public int corePoolSize() {
        return corePoolSize;
    }

    public int maximumPoolSize() {
        return maximumPoolSize;
    }

    /**
     * Create a thread pool config.
     */
    public static ThreadPoolSize of(int corePoolSize, int maximumPoolSize) {
        return new ThreadPoolSize(corePoolSize, maximumPoolSize);
    }

    /**
     * Create a cached thread pool config.
     */
    public static ThreadPoolSize ofCached() {
        return new ThreadPoolSize(-1, -1);
    }

    /**
     * Create a fixed thread pool config.
     */
    public static ThreadPoolSize ofFixed(int size) {
        return new ThreadPoolSize(size, size);
    }

    /**
     * Create a disabled thread pool.
     */
    public static ThreadPoolSize disabled() {
        return new ThreadPoolSize(0, 0);
    }

    @Override
    public String toString() {
        if (isDisabled()) {
            return "0 (disabled)";
        }
        if (isCached()) {
            return "-1 (cached)";
        }
        if (isSingle()) {
            return "1 (single)";
        }
        if (isFixed()) {
            return corePoolSize + " (fixed)";
        }
        if (isDynamic()) {
            return corePoolSize + "-" + maximumPoolSize + " (dynamic)";
        }
        return corePoolSize + "-" + maximumPoolSize + " (unsupported)";
    }

    /**
     * Parse concurrency string into ThreadPoolSize instance.
     * <p>
     * Accepted values:
     * <ul>
     *     <li>0 (disabled)</li>
     *     <li>-1 (cached)</li>
     *     <li>{number} (fixed)</li>
     *     <li>{number}-{number} (dynamic)</li>
     *     <li>virtual (virtual)</li>
     *     <li>virtual:{number} (bounded-virtual)</li>
     * </ul>
     */
    public static ThreadPoolSize parse(String concurrency) {
        if (concurrency.equals("0") || concurrency.equals("-")) {
            return disabled();
        }

        if (concurrency.equals("-1")) {
            return ofCached();
        }

        try {
            final int separatorIndex = concurrency.indexOf('-');
            if (separatorIndex == -1) {
                int size = Integer.parseInt(concurrency, 10);
                return ThreadPoolSize.ofFixed(size);
            }

            return new ThreadPoolSize(parseInt(concurrency, 0, separatorIndex, 10),
                    parseInt(concurrency, separatorIndex + 1, concurrency.length(), 10));
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid concurrency value [" + concurrency + "]: only " +
                    "single maximum integer (e.g. \"5\") and minimum-maximum combo (e.g. \"3-5\") supported, set to -1 to produce a cached thread pool, set to 0 to disable. Start with virtual: to specify virtual thread");
        }
    }

    /**
     * Parses the {@link CharSequence} argument as a signed {@code int} in the specified {@code radix}, beginning at the specified {@code beginIndex}
     * and extending to {@code endIndex - 1}.
     *
     * <p>The method does not take steps to guard against the
     * {@code CharSequence} being mutated while parsing.
     * <p>
     *
     * @param s          the {@code CharSequence} containing the {@code int} representation to be parsed
     * @param beginIndex the beginning index, inclusive.
     * @param endIndex   the ending index, exclusive.
     * @param radix      the radix to be used while parsing {@code s}.
     *
     * @return the signed {@code int} represented by the subsequence in the specified radix.
     * @throws NullPointerException      if {@code s} is null.
     * @throws IndexOutOfBoundsException if {@code beginIndex} is negative, or if {@code beginIndex} is greater than {@code endIndex} or if
     *                                   {@code endIndex} is greater than {@code s.length()}.
     * @throws NumberFormatException     if the {@code CharSequence} does not contain a parsable {@code int} in the specified {@code radix}, or if
     *                                   {@code radix} is either smaller than {@link java.lang.Character#MIN_RADIX} or larger than
     *                                   {@link java.lang.Character#MAX_RADIX}.
     * @since 9
     */
    public static int parseInt(CharSequence s, int beginIndex, int endIndex, int radix)
            throws NumberFormatException {
        Objects.requireNonNull(s);
        if (endIndex > s.length() || beginIndex < 0 || beginIndex > endIndex) {
            throw new IndexOutOfBoundsException();
        }

        if (radix < Character.MIN_RADIX) {
            throw new NumberFormatException("radix " + radix +
                    " less than Character.MIN_RADIX");
        }
        if (radix > Character.MAX_RADIX) {
            throw new NumberFormatException("radix " + radix +
                    " greater than Character.MAX_RADIX");
        }

        boolean negative = false;
        int i = beginIndex;
        int limit = -Integer.MAX_VALUE;

        if (i < endIndex) {
            char firstChar = s.charAt(i);
            if (firstChar < '0') { // Possible leading "+" or "-"
                if (firstChar == '-') {
                    negative = true;
                    limit = Integer.MIN_VALUE;
                } else if (firstChar != '+') {
                    throw new NumberFormatException("Error at index "
                            + (i - beginIndex) + " in: \""
                            + s.subSequence(beginIndex, endIndex) + "\"");
                }
                i++;
                if (i == endIndex) { // Cannot have lone "+" or "-"
                    throw new NumberFormatException("Error at index "
                            + (i - beginIndex) + " in: \""
                            + s.subSequence(beginIndex, endIndex) + "\"");
                }
            }
            int multmin = limit / radix;
            int result = 0;
            while (i < endIndex) {
                // Accumulating negatively avoids surprises near MAX_VALUE
                int digit = Character.digit(s.charAt(i), radix);
                if (digit < 0 || result < multmin) {
                    throw new NumberFormatException("Error at index "
                            + (i - beginIndex) + " in: \""
                            + s.subSequence(beginIndex, endIndex) + "\"");
                }
                result *= radix;
                if (result < limit + digit) {
                    throw new NumberFormatException("Error at index "
                            + (i - beginIndex) + " in: \""
                            + s.subSequence(beginIndex, endIndex) + "\"");
                }
                i++;
                result -= digit;
            }
            return negative ? result : -result;
        } else {
            throw new NumberFormatException("For input string: \"" + s + "\"" +
                    (radix == 10 ? "" : " under radix " + radix));
        }
    }
}
