package com.example.producer;

import java.io.IOException;
import java.util.Arrays;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.SimpleCommandLinePropertySource;

/**
 * Application to publish to Kafka.
 */
@Configuration
@EnableAutoConfiguration
@EnableDiscoveryClient
@ComponentScan(basePackages = {"com.tenx.ms.commons", "com.example.producer"})
public class ProducerApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerApplication.class);

    @Autowired
    private Environment env;

    @Autowired
    private EventProducer eventProducer;

    /**
     * Initializes the micro-service
     * <p/>
     * Spring profiles can be configured with a program arguments --spring.profiles.active=your-active-profile or
     * by setting the environment property SPRING_PROFILES_ACTIVE=your-active-profile
     * <p/>
     *
     * @throws IOException
     */
    @PostConstruct
    public void initApplication() throws IOException {
        if (env.getActiveProfiles().length == 0) {
            LOGGER.warn("No Spring profile configured, running with default configuration");
        } else {
            LOGGER.info("Running with Spring profile(s) : {}", Arrays.toString(env.getActiveProfiles()));
        }

        eventProducer.run();
    }

    /**
     * Set a default profile if it has not been set
     */
    private static void addDefaultProfile(SpringApplication app, SimpleCommandLinePropertySource source) {
        if (!source.containsProperty("spring.profiles.active") && !System.getenv().containsKey("SPRING_PROFILES_ACTIVE")) {
            app.setAdditionalProfiles("dev");
        }
    }

    /**
     * Main method used when the application is run via spring boot
     */
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(ProducerApplication.class);
        app.setBannerMode(Banner.Mode.OFF);
        SimpleCommandLinePropertySource source = new SimpleCommandLinePropertySource(args);

        // Check if the selected profile has been set as argument.
        // if not the development profile will be added
        addDefaultProfile(app, source);
        app.run(args).getEnvironment();
    }
}
