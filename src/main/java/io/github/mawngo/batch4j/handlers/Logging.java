package io.github.mawngo.batch4j.handlers;

import org.slf4j.LoggerFactory;

final class Logging {
    private Logging() {
    }

    public static void error(String format, Object... arguments) {
        LoggerFactory.getLogger(BatchErrorHandler.class).error(format, arguments);
    }
}
