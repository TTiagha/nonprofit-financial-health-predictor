# Nonprofit Financial Health Predictor

## Project Overview

This project aims to develop a predictive model for assessing the financial health of nonprofit organizations using IRS Form 990 data. By leveraging machine learning techniques, we seek to provide insights that can help nonprofits, donors, and policymakers make more informed decisions.

## Project Structure

```
nonprofit-financial-health-predictor/
│
├── .github/
│   └── workflows/    # GitHub Actions workflow files
├── data/             # Data storage and datasets
├── docs/             # Documentation files
├── src/              # Source code for the project
├── tests/            # Test files
│
├── .gitignore        # Specifies intentionally untracked files to ignore
├── Dockerfile        # Defines the Docker image for the project
├── LICENSE           # License file
├── README.md         # Project description and guide (this file)
├── docker-compose.yml # Defines multi-container Docker applications
├── main.py           # Main application entry point
└── requirements.txt  # List of project dependencies
```

## Setup and Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/TTiagha/nonprofit-financial-health-predictor.git
   cd nonprofit-financial-health-predictor
   ```

2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Development

### Running the Application

To run the main application:

```bash
python main.py
```

### Docker

To build and run the application using Docker:

1. Build the Docker image:
   ```bash
   docker build -t nonprofit-predictor .
   ```

2. Run the Docker container:
   ```bash
   docker run nonprofit-predictor
   ```

### Testing

To run the tests:

```bash
python -m unittest discover tests
```

## CI/CD

This project uses GitHub Actions for continuous integration and deployment. The workflow is defined in `.github/workflows/ci_cd.yml`. It performs the following steps:

- Runs tests
- Builds the Docker image
- Runs the application in a Docker container

## Current Status

- Basic project structure is set up
- CI/CD pipeline is implemented and functioning
- Placeholder functions for data loading, processing, and prediction are in place

## Next Steps

- Implement actual data loading from IRS Form 990 datasets
- Develop data processing and feature engineering pipelines
- Research and implement appropriate machine learning models for financial health prediction
- Enhance test coverage and implement more comprehensive unit tests

## Contributing

Contributions to this project are welcome. Please follow these steps:

1. Fork the repository
2. Create a new branch (`git checkout -b feature/AmazingFeature`)
3. Make your changes
4. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
5. Push to the branch (`git push origin feature/AmazingFeature`)
6. Open a Pull Request

## License

This project is licensed under the MIT - see the [LICENSE](LICENSE) file for details.

## Contact

[Your Name] - [Your Email]

Project Link: [https://github.com/TTiagha/nonprofit-financial-health-predictor](https://github.com/TTiagha/nonprofit-financial-health-predictor)
```

This README now reflects the current state of your project, including the CI/CD setup, Docker configuration, and the progress you've made. It also outlines the next steps for development, which can serve as a roadmap for you and potential contributors.

Remember to replace `[Your Name]`, `[Your Email]`, and `[LICENSE NAME]` with the appropriate information. Also, make sure the GitHub repository link is correct.

You can further customize this README as your project evolves, adding more detailed usage instructions, contribution guidelines, or any other relevant information.