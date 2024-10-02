# Nonprofit Financial Health Predictor

## Project Overview

This project aims to develop a predictive model for assessing the financial health of nonprofit organizations using IRS Form 990 data. By leveraging machine learning techniques, we seek to provide insights that can help nonprofits, donors, and policymakers make more informed decisions.

## Key Features

- Data Processing: High-speed XML processing using AWS CloudShell
- Flexible Analysis: User can filter by state and select specific IRS data ZIP files
- Comprehensive Metrics: Analyzes key financial indicators (Total Assets, Revenue, Expenses, Net Assets)
- Data Storage: Efficient storage using Parquet format in Amazon S3
- Monitoring: CloudWatch alarms for process monitoring and cost management
- Logging: Detailed logging in CloudWatch for error tracking and run summaries
- Automated IRS Form 990 XML File Tracking: Monitors and reports new file releases
- NTEE Code Interpretation: Uses AI to infer NTEE (National Taxonomy of Exempt Entities) codes from organization names and mission statements
- Visualization: Generates insightful visualizations of the processed data, including NTEE code distribution

## Recent Updates

- Switched from Mistral AI to OpenAI's GPT-4-mini for NTEE code interpretation
- Removed BusinessActivityCode field from data processing to streamline analysis
- Added NTEE code interpretation using AI for better categorization of nonprofits
- Improved XML parsing robustness using namespace-agnostic XPath expressions
- The application now allows users to specify the state to filter for and input multiple URLs for processing
- Implemented CloudWatch logging for specific, important log messages
- Removed local file logging to health.log
- Added new990.py script for automated tracking of new IRS Form 990 XML file releases
- Implemented new visualization for NTEE Code Description distribution

## Data Processing Insights

We've learned the importance of using local-name() in XPath expressions when parsing XML files. This approach makes our expressions more namespace-agnostic, increasing extraction rates and improving the robustness of our data processing pipeline. For example:

```python
//*[local-name()="TotalAssetsEOYAmt"]/text()
```

This method allows our parser to find the correct elements regardless of namespace prefixes, making our code more adaptable to variations in XML structure.

## Tech Stack

- AWS Services: S3, CloudWatch, CloudShell
- Data Processing: Python (pandas, pyarrow)
- Data Format: XML parsing, Parquet for storage
- Web Scraping: BeautifulSoup4 for parsing IRS website
- AI: OpenAI's GPT-4-mini for NTEE code interpretation
- Data Visualization: Matplotlib, Seaborn

## Project Structure

nonprofit-financial-health-predictor/
│
├── .github/
│   └── workflows/    # GitHub Actions workflow files
├── data/             # Data storage and datasets
├── docs/             # Documentation files
├── src/              # Source code for the project
│   ├── main.py           # Main application entry point
│   ├── config.py         # Configuration settings
│   ├── data_processor.py # Data processing functions
│   ├── data_analyzer.py  # Data analysis functions
│   ├── logger.py         # Logging configuration
│   ├── s3_utils.py       # Amazon S3 utilities
│   ├── utils.py          # Utility functions
│   ├── xml_downloader.py # XML file downloading and extraction
│   ├── xml_parser.py     # XML parsing functions
│   ├── new990.py         # IRS Form 990 XML file tracking script
│   └── visualization/    # Data visualization scripts
│       └── DataVisualization.py # Generates visualizations
├── tests/            # Test files
│
├── .gitignore        # Specifies intentionally untracked files to ignore
├── Dockerfile        # Defines the Docker image for the project
├── LICENSE           # License file
├── README.md         # Project description and guide (this file)
├── docker-compose.yml # Defines multi-container Docker applications
└── requirements.txt  # List of project dependencies

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

3. Set up AWS credentials for CloudWatch logging:
   - Ensure you have the AWS CLI installed and configured with the necessary permissions for CloudWatch Logs.
   - Run `aws configure` and provide your AWS Access Key ID, Secret Access Key, and default region.

4. Set up OpenAI API key:
   - Obtain an API key from OpenAI
   - Replace 'your-api-key-here' in src/main.py with your actual OpenAI API key

## Usage

### Main Application

To run the main application:

```bash
python src/main.py
```

When prompted:
1. Enter the state abbreviation you want to filter for (e.g., "CA" for California).
2. Enter one or more URLs to process, separated by commas.

The application will then:
- Download and extract XML files from the provided URLs.
- Process the XML files, filtering for nonprofits in the specified state.
- Analyze the data and provide statistics on various financial metrics.
- Infer NTEE codes for each nonprofit using AI.
- Save the processed data to a Parquet file and upload it to Amazon S3.
- Log important information to CloudWatch Logs.

### IRS Form 990 XML File Tracker

To check for new IRS Form 990 XML file releases:

```bash
python src/new990.py
```

This script will:
- Check the IRS website for new Form 990 XML file releases.
- Report any new files found since the last check.
- Update the last checked year to avoid redundant processing.

### Data Visualization

To generate visualizations:

```bash
python src/visualization/DataVisualization.py
```

This script will create several visualizations:
- Revenue Distribution
- Top 10 Organizations by Total Assets
- Average Financial Metrics by Form Type
- Expense to Revenue Ratio Distribution
- NTEE Code Description Distribution

The visualizations will be saved as PNG files in the current directory.

## CloudWatch Logging

The application logs specific, important messages to AWS CloudWatch Logs. These include:
- Processing statistics (e.g., number of records processed, processing time)
- File statistics (e.g., files without certain financial data)
- Data analysis results (e.g., average fields per record, financial metrics statistics)
- S3 upload confirmations
- NTEE code interpretation results

To view these logs:
1. Go to the AWS CloudWatch console.
2. Navigate to "Logs" > "Log groups".
3. Find the log group named "NonprofitFinancialHealthPredictor".
4. Click on the log stream named "ApplicationLogs" to view the logged messages.

## Sample Output

Processed 84 RI nonprofit records from 21513 files in 105.49 seconds
Files without TotalRevenue: 0
Files without TotalExpenses: 0
Files without TotalAssets: 0
Files without TotalNetAssets: 0
Average fields per record: 19.67
Form type distribution: {'990PF': 12, '990EZ': 17, '990': 53, '990T': 2}
TotalNetAssets: min=-1426990.0, max=111192741.0, avg=4526458.928571428
TotalAssets: min=0.0, max=111238696.0, avg=5387384.130952381
TotalRevenue: min=-74151.0, max=61455293.0, avg=1827346.619047619
TotalExpenses: min=0.0, max=60192780.0, avg=1672105.2142857143
Top 5 inferred NTEE codes: [('Education', 15), ('Human Services', 12), ('Health', 10), ('Arts and Culture', 8), ('Environment', 7)]

## Development

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

- Project structure has been reorganized for better maintainability.
- The application now supports filtering by any state and processing multiple URLs.
- Basic data processing, analysis, and S3 upload functionality is implemented.
- CI/CD pipeline is implemented and functioning.
- CloudWatch logging for important messages has been implemented.
- Local file logging has been removed.
- Automated tracking of new IRS Form 990 XML file releases is implemented.
- BusinessActivityCode field has been removed from data processing.
- NTEE code interpretation using AI (OpenAI's GPT-4-mini) has been added.
- XML parsing has been improved with namespace-agnostic XPath expressions.
- New visualization for NTEE Code Description distribution has been added.

## Next Steps

- Implement more comprehensive data analysis and visualization.
- Develop machine learning models for financial health prediction.
- Enhance test coverage and implement more unit tests.
- Improve error handling and input validation.
- Add more detailed documentation for each module.
- Refine NTEE code interpretation accuracy.
- Explore additional visualizations to provide deeper insights into nonprofit financial health.

## Contributing

Contributions to this project are welcome. Please follow these steps:

1. Fork the repository
2. Create a new branch (`git checkout -b feature/AmazingFeature`)
3. Make your changes
4. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
5. Push to the branch (`git push origin feature/AmazingFeature`)
6. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

[Your Name] - [Your Email]

Project Link: [https://github.com/TTiagha/nonprofit-financial-health-predictor](https://github.com/TTiagha/nonprofit-financial-health-predictor)