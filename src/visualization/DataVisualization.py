import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Assuming you've exported your Athena query results to CSV files
revenue_distribution = pd.read_csv('revenue_distribution.csv')
top_organizations = pd.read_csv('top_organizations.csv')
form_type_metrics = pd.read_csv('form_type_metrics.csv')
expense_ratio = pd.read_csv('expense_ratio.csv')

# Set the style for all plots
plt.style.use('seaborn-v0_8')

# 1. Revenue Distribution
plt.figure(figsize=(12, 6))
sns.barplot(x='RevenueBracket', y='Count', data=revenue_distribution)
plt.title('Distribution of Nonprofits by Revenue Bracket')
plt.xlabel('Revenue Bracket')
plt.ylabel('Number of Nonprofits')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('revenue_distribution.png')
plt.close()

# 2. Top 10 Organizations by Total Assets
plt.figure(figsize=(12, 6))
sns.barplot(x='OrganizationName', y='TotalAssets', data=top_organizations)
plt.title('Top 10 Nonprofits by Total Assets')
plt.xlabel('Organization Name')
plt.ylabel('Total Assets ($)')
plt.xticks(rotation=90)
plt.tight_layout()
plt.savefig('top_organizations.png')
plt.close()

# 3. Average Financial Metrics by Form Type
metrics = ['AvgRevenue', 'AvgExpenses', 'AvgAssets', 'AvgNetAssets']
form_type_metrics_melted = pd.melt(form_type_metrics, id_vars=['FormType'], value_vars=metrics, var_name='Metric', value_name='Amount')

plt.figure(figsize=(12, 6))
sns.barplot(x='FormType', y='Amount', hue='Metric', data=form_type_metrics_melted)
plt.title('Average Financial Metrics by Form Type')
plt.xlabel('Form Type')
plt.ylabel('Amount ($)')
plt.legend(title='Metric')
plt.tight_layout()
plt.savefig('form_type_metrics.png')
plt.close()

# 4. Expense to Revenue Ratio Distribution
plt.figure(figsize=(12, 6))
sns.barplot(x='ExpenseToRevenueRatio', y='Count', data=expense_ratio)
plt.title('Distribution of Expense to Revenue Ratio')
plt.xlabel('Expense to Revenue Ratio')
plt.ylabel('Number of Nonprofits')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('expense_ratio_distribution.png')
plt.close()

print("Visualizations have been saved as PNG files.")