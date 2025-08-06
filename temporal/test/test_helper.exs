ExUnit.start()

# Ensure HTTPoison is started for tests
Application.ensure_all_started(:httpoison)

# Set up Mox for testing
Mox.defmock(Temporal.Native.Mock, for: Temporal.Native.Behaviour)

# Load test support modules
Code.require_file("support/temporal_test_container.ex", __DIR__)
Code.require_file("support/temporal_mocks.ex", __DIR__)

# Start the container state agent
Temporal.TestContainer.start_link()
