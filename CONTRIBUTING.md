# Contribution Guide

Thank you for considering contributing to **CyberReportHub (crh)**! We welcome contributions from everyone, whether you're fixing bugs, suggesting features, or improving documentation. Please follow the steps outlined below to ensure a smooth process.

## Table of Contents
- Code of Conduct 
- How to Contribute
  - Reporting Bugs
  - Proposing New Features
  - Submitting Code
- Development Setup
- Style Guides
- Licensing

---

## Code of Conduct 
We expect participants to:
- Be respectful and kind to others.
- Be considerate of different perspectives and experiences.
- Communicate openly and constructively, with empathy.
- Collaborate in good faith and with the intent to build a positive, inclusive community.
- Encourage diverse participation and help others feel welcome.

Examples of unacceptable behavior include:
- Harassment, intimidation, or discrimination of any kind.
- Offensive comments related to race, gender, sexual orientation, disability, physical appearance, or religion.
- Personal attacks or name-calling.
- Disruptive behavior, including trolling or unconstructive criticism.
- Publishing others' private information without consent.
- Any form of abuse, violence, or threat.

---

## How to Contribute

There are several ways you can contribute to **CyberReportHub**:

### Reporting Bugs

1. **Check existing issues**: Before opening a new bug report, make sure your issue hasn't been reported yet.
2. **Create a new issue**: If you find a bug, open a new issue in our Issues section. Please use the templates found in the `.github/ISSUE_TEMPLATE` directory depending on the type of issue. 

### Proposing New Features

We welcome new feature ideas! If you'd like to propose one, please follow these steps:

1. **Check for existing feature requests**: Before submitting a new feature proposal, ensure that someone else hasn't already requested it.
2. **Submit a feature request**: Open a new issue and follow `.github/PULL_REQUEST_TEMPLATE.md`. 

### Submitting Code

We accept code contributions via pull requests (PRs). To ensure that your changes align with the project, follow these steps:

1. **Fork the repository**: Click the "Fork" button at the top of the repository page to create a copy of the project under your GitHub account.
2. **Clone your fork**: Clone the forked repository to your local machine.
   ```bash
   git clone https://github.com/yourusername/project.git
   cd project
   ```
3. **Create a new branch**: Always create a new branch for your changes.
   ```bash
   git checkout -b feature-branch-name
   ```
4. **Make your changes**: Implement your changes in the code, ensuring they follow the project's coding standards and style guides.
5. **Commit your changes**: Use clear, concise commit messages to describe your changes.
   ```bash
   git commit -m "Fix issue #123: Description of the fix"
   ```
6. **Push your branch**: Push your changes to your fork.
   ```bash
   git push origin feature-branch-name
   ```
7. **Open a pull request (PR)**: Go to the original repository and open a PR. Provide a clear description of what you’ve done, referencing any relevant issues.

---

## Development Setup

To get started developing on **CyberReportHub**, you'll need to set up your local environment. Follow these steps:

1. **Clone the repositories**: Use the `clone-repo` script found in the developer-environment repository to clone all the microservice repositories.

2. **Update the repositories**: Use the `update-repo` script found in the developer-environment repository to update all the microservice repositories..

3. **Set up environment variables**: If you would like to customize specific environment variables, make sure to create a `.env` file or set them manually. If not, the default values will be used.

4. **Run the application**: Use the `setup-docker` script found in the developer-environment repository to set up the Docker containers for all the microservice repositories. Alternatively, follow the instructions in the `README.md` for running each microservice application locally. 

---

## Style Guides

Please adhere to the following style guides when contributing code to ensure consistency across the project:

- **Code formatting**: Follow the standard code formatting guidelines for the project. For example:
  - Python: [PEP 8](https://peps.python.org/pep-0008/)
  - Java: [Spotless](https://github.com/diffplug/spotless)

- **Commit messages**: Follow the [Conventional Commits](https://www.conventionalcommits.org/) standard for commit messages. Example:
  ```
  feat: Add new user authentication API
  fix: Correct typo in README
  ```

---

## Licensing

By contributing to CyberReportHub, you agree that your contributions will be licensed under the project's LICENSE file.
By contributing to CyberReportHub, you agree that your contributions will be licensed under the project's MIT license. Please see the LICENSE file for more details.

---

Thank you for contributing to CyberReportHub! We appreciate your help in making this project better.