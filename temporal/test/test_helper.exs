ExUnit.start()

# Ensure HTTPoison is started for tests
Application.ensure_all_started(:httpoison)

# Load test support modules
Code.require_file("support/temporal_test_container.ex", __DIR__)

# Start the container state agent
Temporal.TestContainer.start_link()
