# config.py

import boto3

# AWS S3 configurations
S3_BUCKET = 'nonprofit-financial-health-data'
S3_FOLDER = 'irs990-data'
S3_NOREV_FOLDER = 'NoRev'
S3_NOEXP_FOLDER = 'NoExp'
S3_NOASS_FOLDER = 'NoAss'
S3_NONASS_FOLDER = 'NoNAss'

s3_client = boto3.client('s3')

# Desired fields to extract from XML
desired_fields = {
    'State': {
        'type': 'string',
        'paths': {
            'Common': [
                '//*[local-name()="ReturnHeader"]/*[local-name()="Filer"]/*[local-name()="USAddress"]/*[local-name()="StateAbbreviationCd"]/text()',
                '//*[local-name()="ReturnHeader"]/*[local-name()="Filer"]/*[local-name()="BusinessOfficeGrp"]/*[local-name()="USAddress"]/*[local-name()="StateAbbreviationCd"]/text()',
                '//*[contains(local-name(), "StateAbbreviationCd")]/text()'
            ]
        }
    },
    'EIN': {
        'type': 'string',
        'paths': {
            'Common': [
                '//*[local-name()="ReturnHeader"]/*[local-name()="Filer"]/*[local-name()="EIN"]/text()'
            ]
        }
    },
    'TaxYear': {
        'type': 'int',
        'paths': {
            'Common': [
                '//*[local-name()="ReturnHeader"]/*[local-name()="TaxYr"]/text()',
                '//*[local-name()="ReturnHeader"]/*[local-name()="TaxYear"]/text()',
                'substring(//*[local-name()="ReturnHeader"]/*[local-name()="TaxPeriodEndDt"]/text(),1,4)'
            ]
        }
    },
    'TaxPeriodEndDt': {
        'type': 'string',
        'paths': {
            'Common': [
                '//*[local-name()="ReturnHeader"]/*[local-name()="TaxPeriodEndDt"]/text()',
            ]
        }
    },
    'OrganizationName': {
        'type': 'string',
        'paths': {
            'Common': [
                '//*[local-name()="ReturnHeader"]/*[local-name()="Filer"]/*[local-name()="BusinessName"]/*[local-name()="BusinessNameLine1Txt"]/text()',
                '//*[local-name()="ReturnHeader"]/*[local-name()="Filer"]/*[local-name()="Name"]/*[local-name()="BusinessNameLine1Txt"]/text()'
            ]
        }
    },
    'TotalRevenue': {
        'type': 'double',
        'paths': {
            '990': [
                '//*[local-name()="ReturnData"]/*[local-name()="IRS990"]/*[local-name()="TotalRevenueAmt"]/text()',
                '//*[local-name()="ReturnData"]/*[local-name()="IRS990"]/*[local-name()="Revenue"]/*[local-name()="TotalRevenueAmt"]/text()',
                '//*[local-name()="ReturnData"]/*[local-name()="IRS990"]/*[local-name()="Revenue"]/*[local-name()="TotalRevenueColumnAmt"]/text()',
                '//*[local-name()="ReturnData"]/*[local-name()="IRS990"]/*[local-name()="Form990PartVIII"]/*[local-name()="TotalRevenueColumnAmt"]/text()',
            ],
            '990EZ': [
                '//*[local-name()="ReturnData"]/*[local-name()="IRS990EZ"]/*[local-name()="TotalRevenueAmt"]/text()'
            ],
            '990PF': [
                '//*[local-name()="ReturnData"]/*[local-name()="IRS990PF"]/*[local-name()="TotalRevenueAndExpensesAmt"]/text()',
                '//*[local-name()="ReturnData"]/*[local-name()="IRS990PF"]/*[local-name()="AnalysisOfRevenueAndExpenses"]/*[local-name()="TotalRevAndExpnssAmt"]/text()'
            ],
            '990T': [
                '//*[local-name()="ReturnData"]/*[local-name()="IRS990T"]/*[local-name()="TotalUBTIAmt"]/text()'
            ],
            'Common': [
                '//*[contains(local-name(), "TotalRevenue")]/text()',
            ]
        }
    },
    'TotalExpenses': {
        'type': 'double',
        'paths': {
            '990': [
                '//*[local-name()="TotalFunctionalExpensesAmt"]/text()',
                '//*[local-name()="TotalExpensesAmt"]/text()',
                '//*[local-name()="TotalFunctionalExpenses"]/text()',
                '//*[local-name()="TotalExpenses"]/text()',
                '//*[contains(local-name(), "TotalFunctionalExpenses") or contains(local-name(), "TotalExpenses")]/text()',
            ],
            '990EZ': [
                '//*[local-name()="TotalExpensesAmt"]/text()',
                '//*[local-name()="TotalExpenses"]/text()',
                '//*[contains(local-name(), "TotalExpenses")]/text()',
            ],
            '990PF': [
                '//*[local-name()="TotalExpensesAndDisbursementsAmt"]/text()',
                '//*[local-name()="TotalExpensesRevAndExpnssAmt"]/text()',
                '//*[local-name()="TotalExpenses"]/text()',
                '//*[contains(local-name(), "TotalExpenses")]/text()',
            ],
            '990T': [
                '//*[local-name()="TotalDeductionAmt"]/text()',
                '//*[local-name()="TotalDeductionsAmt"]/text()',
                '//*[local-name()="TotalDeductions"]/text()',
                '//*[local-name()="TotalDeduction"]/text()',
                '//*[local-name()="TotalExpenses"]/text()',
                '//*[contains(local-name(), "TotalExpenses") or contains(local-name(), "TotalDeductions")]/text()',
            ],
            'Common': [
                '//*[contains(local-name(), "TotalExpenses")]/text()',
                '//*[contains(local-name(), "TotalDeductions")]/text()',
            ]
        }
    },
    'TotalAssets': {
        'type': 'double',
        'paths': {
            '990': [
                '//*[local-name()="TotalAssetsEOYAmt"]/text()',
                '//*[local-name()="TotalAssetsEndOfYear"]/text()',
                '//*[local-name()="AssetsEOYAmt"]/text()',
                '//*[local-name()="AssetsEOY"]/text()',
                '//*[local-name()="TotalAssets"]/text()',
                '//*[contains(local-name(), "TotalAssets") and contains(local-name(), "EOY")]/text()',
                '//*[contains(local-name(), "TotalAssets")]/text()',
            ],
            '990EZ': [
                '//*[local-name()="Form990TotalAssetsGrp"]/*[local-name()="EOYAmt"]/text()',
                '//*[local-name()="TotalAssetsEOYAmt"]/text()',
                '//*[local-name()="AssetsEOYAmt"]/text()',
                '//*[local-name()="TotalAssets"]/text()',
                '//*[contains(local-name(), "TotalAssets") and contains(local-name(), "EOY")]/text()',
            ],
            '990PF': [
                '//*[local-name()="TotalAssetsEOYAmt"]/text()',
                '//*[local-name()="FMVAssetsEOYAmt"]/text()',
                '//*[local-name()="TotalAssets"]/text()',
                '//*[contains(local-name(), "TotalAssets") and contains(local-name(), "EOY")]/text()',
                '//*[contains(local-name(), "FMVAssets") and contains(local-name(), "EOY")]/text()',
            ],
            '990T': [
                '//*[local-name()="BookValueAssetsEOYAmt"]/text()',
                '//*[local-name()="TotalAssetsEOYAmt"]/text()',
                '//*[local-name()="TotalAssets"]/text()',
                '//*[contains(local-name(), "TotalAssets") and contains(local-name(), "EOY")]/text()',
            ],
            'Common': [
                '//*[contains(local-name(), "TotalAssets") and contains(local-name(), "EOY")]/text()',
                '//*[contains(local-name(), "AssetsEOY")]/text()',
                '//*[contains(local-name(), "TotalAssets")]/text()',
                '//*[contains(local-name(), "FMVAssets") and contains(local-name(), "EOY")]/text()',
                '//*[contains(local-name(), "BookValueAssets") and contains(local-name(), "EOY")]/text()',
            ]
        }
    },
    'MissionStatement': {
        'type': 'string',
        'paths': {
            '990': [
                '//*[local-name()="MissionDesc"]/text()',
                '//*[local-name()="MissionStatement"]/text()',
                '//*[local-name()="MissionStatementTxt"]/text()',
                '//*[local-name()="ActivityOrMissionDesc"]/text()',
                '//*[local-name()="PrimaryExemptPurposeTxt"]/text()',
                '//*[contains(local-name(), "Mission") and contains(local-name(), "Desc")]/text()',
                '//*[contains(local-name(), "ExemptPurpose")]/text()',
                '//*[contains(local-name(), "MissionStatement")]/text()'
            ],
            '990EZ': [
                '//*[local-name()="MissionDesc"]/text()',
                '//*[local-name()="MissionStatement"]/text()',
                '//*[local-name()="PrimaryExemptPurposeTxt"]/text()',
                '//*[contains(local-name(), "Mission") and contains(local-name(), "Desc")]/text()',
            ],
            '990PF': [
                '//*[local-name()="MissionDesc"]/text()',
                '//*[local-name()="MissionStatement"]/text()',
                '//*[local-name()="ActivityOrMissionDesc"]/text()',
                '//*[local-name()="PrimaryExemptPurposeTxt"]/text()',
                '//*[contains(local-name(), "ExemptPurpose")]/text()',
            ],
            '990T': [
                '//*[local-name()="MissionDesc"]/text()',
                '//*[local-name()="MissionStatement"]/text()',
                '//*[local-name()="ActivityOrMissionDesc"]/text()',
            ],
            'Common': [
                '//*[contains(local-name(), "Mission") and (contains(local-name(), "Desc") or contains(local-name(), "Statement") or contains(local-name(), "Txt"))]/text()',
                '//*[contains(local-name(), "ExemptPurpose")]/text()',
                '//*[contains(local-name(), "ActivityOrMissionDesc")]/text()',
                '//*[contains(local-name(), "PrimaryExemptPurposeTxt")]/text()'
            ]
        }
    },
    'TotalNetAssets': {
        'type': 'double',
        'paths': {
            '990': [
                '//*[local-name()="TotalNetAssetsFundBalanceEOYAmt"]/text()',
                '//*[local-name()="TotNetAssetsFundBalanceEOYAmt"]/text()',
                '//*[local-name()="NetAssetsFundBalancesEOYAmt"]/text()',
                '//*[local-name()="NetAssetsOrFundBalancesEOYAmt"]/text()',
                '//*[local-name()="NetAssetsEOYAmt"]/text()',
                '//*[contains(local-name(), "TotalNetAssets") and contains(local-name(), "EOY")]/text()',
                '//*[contains(local-name(), "NetAssets") and contains(local-name(), "EOY")]/text()',
            ],
            '990EZ': [
                '//*[local-name()="TotalNetAssetsEOYAmt"]/text()',
                '//*[local-name()="NetAssetsOrFundBalancesEOYAmt"]/text()',
                '//*[contains(local-name(), "NetAssets") and contains(local-name(), "EOY")]/text()',
            ],
            '990PF': [
                '//*[local-name()="TotNetAstOrFundBalancesEOYAmt"]/text()',
                '//*[local-name()="TotalNetAssetsEOYAmt"]/text()',
                '//*[local-name()="NetAssetsOrFundBalancesEOYAmt"]/text()',
                '//*[contains(local-name(), "NetAssets") and contains(local-name(), "EOY")]/text()',
            ],
            '990T': [
                '//*[local-name()="BookValueAssetsEOYAmt"]/text()',
            ],
            'Common': [
                '//*[contains(local-name(), "TotalNetAssets") and contains(local-name(), "EOY")]/text()',
                '//*[contains(local-name(), "NetAssets") and contains(local-name(), "EOY")]/text()',
                '//*[contains(local-name(), "TotNetAstOrFundBalancesEOYAmt")]/text()',
                '//*[contains(local-name(), "NetAssetsOrFundBalancesEOYAmt")]/text()',
            ]
        }
    },
}
