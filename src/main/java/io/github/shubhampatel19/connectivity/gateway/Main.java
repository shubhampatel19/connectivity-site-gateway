package io.github.shubhampatel19.connectivity.gateway;

import java.time.Clock;

/** Entry point: reads configuration, starts the gateway, stops it cleanly on SIGTERM. */
public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        Log log = Log.root("site-gateway");
        Config cfg;
        try {
            cfg = Config.fromEnv(System.getenv());
        } catch (IllegalArgumentException e) {
            log.error("exiting", "err", e.getMessage());
            System.exit(1);
            return;
        }
        Gateway gateway = Gateway.start(cfg, log, Clock.systemUTC());
        Runtime.getRuntime().addShutdownHook(new Thread(gateway::stop, "shutdown"));
        Thread.currentThread().join(); // the HTTP server's threads are daemons; keep the JVM up
    }
}
