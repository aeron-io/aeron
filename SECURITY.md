# Security Policy

The Aeron project takes security seriously. Aeron is maintained by [Adaptive](https://weareadaptive.com).

## Reporting a Vulnerability

Please report suspected security vulnerabilities using **[GitHub Security Advisories](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing/privately-reporting-a-security-vulnerability)**.

If you are unable to use GitHub Security Advisories, you can email security@weareadaptive.com.

Please don't report a suspected vulnerability through a public GitHub issue, pull request, or discussion — a public report can expose users before a fix or mitigation exists.

Where possible, please include:

- Affected component(s) — Transport, Archive, or Cluster — and version
- A description of the issue and its potential impact
- Reproduction steps or a minimal proof of concept
- Any evidence of active exploitation

We will acknowledge reports. Acknowledgement isn't a commitment that the report is valid, that a fix will be provided, or to a disclosure date.

## Classifying reports

Not every defect is a security vulnerability. When triaging a report, maintainers ask whether it affects:

- **Confidentiality** — does it leak data, secrets, or internal system details to someone who shouldn't have them?
- **Integrity** — does it let someone modify data or state, or bypass an authorization or validation check, that they shouldn't be able to?
- **Availability** — could it crash, hang, or meaningfully degrade the system for others?

and whether it does so by crossing a trust boundary or bypassing a control Aeron itself provides (see [deployment assumptions](#scope-and-deployment-assumptions)), rather than requiring an adversary or capability outside that trust model — a compromised host, OS, credentials, or network; a misconfiguration; or a deployment that contradicts those assumptions. Reports that don't meet this bar are usually hardening suggestions, ordinary bugs, or out of scope.

This also excludes automated scanner findings that simply flag an already-known CVE in a dependency — please report those to the dependency's own project, not as an Aeron vulnerability.

## Scope and deployment assumptions

- **Aeron does not authenticate or encrypt transport traffic by default.** That's fine on a trusted, isolated network — not on an untrusted one. On untrusted networks, including most cloud deployments, enable [Aeron Transport Security (ATS)](https://aeron.io) (Aeron Premium) or apply your own network-layer encryption (IPsec, WireGuard).
- **Aeron Archive and Aeron Cluster both support authentication and authorization** for controlling who can connect and what they can do — both are disabled by default. Where configured, a bypass of these controls is treated as a vulnerability, not a deployment issue.
- **Operators are responsible for securing the host, OS, credentials, access controls, and network Aeron runs on**, beyond the Archive/Cluster controls above. This is particularly important for Cluster if it's deployed with client gateways in a different trust zone (e.g. on-prem Cluster, cloud-hosted clients) rather than one uniformly trusted network. A compromised host or trusted network isn't an Aeron vulnerability by itself.
- This is the trust model Aeron is designed around.

## Triage and handling

Maintainers, with Adaptive's security function where relevant, determine:

1. Whether it affects Aeron itself, or an external dependency/deployment choice;
2. Whether it meets the vulnerability bar above;
3. Affected versions and deployment scenarios;
4. Severity and practical exploitability;
5. The right response — fix, mitigation, documentation, or none; and
6. Whether a public advisory is warranted.

Fixes must preserve Aeron's correctness and performance characteristics where reasonably possible; hot-data-path issues may need extra design/performance review.

We won't disclose report details publicly while remediation is in progress, except where required by law or necessary to protect users.

## Security advisories, CVEs, and disclosure

Once a vulnerability is confirmed, we follow a coordinated disclosure sequence:

1. Develop and review the fix privately, in the draft GitHub Security Advisory.
2. Ship the fix in a release.
3. Publish the advisory, requesting a CVE where warranted.
4. The CVE is indexed into public vulnerability databases (e.g. NVD) once assigned.

An advisory may include affected component(s), affected/fixed versions, severity and impact, a description, mitigation/upgrade guidance, and credit to the reporter (unless they prefer anonymity). Credit is based on the details provided in the original report; we generally won't update it in response to later requests. We won't publish exploit details that would put un-upgraded users at unnecessary risk.

### EU Cyber Resilience Act reporting

Separately from the disclosure process above: if a vulnerability is confirmed to be **actively exploited**, or an incident meets the [EU Cyber Resilience Act](https://eur-lex.europa.eu/eli/reg/2024/2847/oj/eng)'s threshold for a severe security incident, Adaptive additionally reports it to the relevant EU authorities — a 24-hour early warning, a 72-hour notification, and a final report once a fix is available. This is triggered by **evidence of exploitation, not by knowledge of a vulnerability alone**, and runs alongside, not instead of, the public disclosure process above.

## Supported versions

For the open-source project:

- Upgrade to the latest release where practical — the primary remediation path for free/OSS users.
- A fix may land in the next ordinary release rather than an emergency one.
- Backports to older release lines aren't guaranteed unless stated in the release notes or advisory.

Extended support for older versions, including backported security fixes, is available under **Aeron Premium** — contact your account team for details.
