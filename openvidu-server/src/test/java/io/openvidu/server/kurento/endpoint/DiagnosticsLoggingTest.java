package io.openvidu.server.kurento.endpoint;

import static org.junit.Assert.*;

import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.Test;
import org.springframework.boot.logging.LoggingInitializationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;

public class DiagnosticsLoggingTest {

	private LoggerContext configure(String path) throws Exception {
		StandardEnvironment environment = new StandardEnvironment();
		environment.getPropertySources().addFirst(new MapPropertySource("test",
				Collections.singletonMap("OPENVIDU_DIAGNOSTICS_PATH", path)));
		Class<?> type = Class.forName("org.springframework.boot.logging.logback.SpringBootJoranConfigurator");
		Constructor<?> constructor = type.getDeclaredConstructor(LoggingInitializationContext.class);
		constructor.setAccessible(true);
		JoranConfigurator configurator = (JoranConfigurator) constructor.newInstance(new LoggingInitializationContext(environment));
		LoggerContext context = new LoggerContext();
		configurator.setContext(context);
		configurator.doConfigure(getClass().getClassLoader().getResource("logback-spring.xml"));
		return context;
	}

	@Test public void optionalPersistentLogWritesToConfiguredDirectory() throws Exception {
		Path path = Files.createTempDirectory("ov-diagnostics-logging-");
		LoggerContext context = configure(path.toString());
		try {
			context.getLogger(MediaLifecycleDiagnostics.class).info("MEDIA_LIFECYCLE test marker");
			assertTrue(Files.readString(path.resolve("openvidu.log"), StandardCharsets.UTF_8)
					.contains("MEDIA_LIFECYCLE test marker"));
		} finally {
			context.stop();
		}
	}

	@Test public void persistentLogIsDisabledByDefault() throws Exception {
		LoggerContext context = configure("");
		try {
			assertNull(context.getLogger("ROOT").getAppender("DIAGNOSTICS"));
		} finally {
			context.stop();
		}
	}
}
