# Security

This project is experimental. Security fixes target the current development
version; there is no maintained stable release line yet.

Use the repository's **Security → Report a vulnerability** option for private
reports when it is enabled. If that option is unavailable, open an issue asking
for a private reporting channel without including vulnerability details,
credentials, or telemetry samples. Do not disclose exploitable details in public
issues or pull requests before coordinating with the maintainer.

Include the affected revision, reproduction steps using synthetic data, expected
behavior, and the potential impact. Reports about unintended telemetry loss,
credential disclosure, or inference requests containing unexpected data are
relevant security concerns.

Before operating the processor, review the README's data handling and retention
policy sections. API keys belong in environment variables or your deployment's
secret management, not committed configuration files.
