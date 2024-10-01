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
                'irs:ReturnHeader/irs:Filer/irs:USAddress/irs:StateAbbreviationCd/text()',
                'irs:ReturnHeader/irs:Filer/irs:BusinessOfficeGrp/irs:USAddress/irs:StateAbbreviationCd/text()',
                '//*[contains(local-name(), "StateAbbreviationCd")]/text()'
            ]
        }
    },
    'EIN': {
    'type': 'string',  # Changed from 'int' to 'string'
    'paths': {
        'Common': [
            'irs:ReturnHeader/irs:Filer/irs:EIN/text()'
        ]
    }
},
    'TaxYear': {  # Changed from 'TaxYr' to 'TaxYear'
        'type': 'int',
        'paths': {
            'Common': [
                'irs:ReturnHeader/irs:TaxYr/text()',
                'irs:ReturnHeader/irs:TaxYear/text()',
                'substring(irs:ReturnHeader/irs:TaxPeriodEndDt/text(),1,4)'
            ]
        }
    },
    'TaxPeriodEndDt': {  # Added 'TaxPeriodEndDt'
        'type': 'string',
        'paths': {
            'Common': [
                'irs:ReturnHeader/irs:TaxPeriodEndDt/text()',
            ]
        }
    },
    'OrganizationName': {
        'type': 'string',
        'paths': {
            'Common': [
                'irs:ReturnHeader/irs:Filer/irs:BusinessName/irs:BusinessNameLine1Txt/text()',
                'irs:ReturnHeader/irs:Filer/irs:Name/irs:BusinessNameLine1Txt/text()'
            ]
        }
    },
    'TotalRevenue': {
        'type': 'double',
        'paths': {
            '990': [
                'irs:ReturnData/irs:IRS990/irs:TotalRevenueAmt/text()',
                'irs:ReturnData/irs:IRS990/irs:Revenue/irs:TotalRevenueAmt/text()',
                'irs:ReturnData/irs:IRS990/irs:Revenue/irs:TotalRevenueColumnAmt/text()',
                'irs:ReturnData/irs:IRS990/irs:Form990PartVIII/irs:TotalRevenueColumnAmt/text()',
            ],
            '990EZ': [
                'irs:ReturnData/irs:IRS990EZ/irs:TotalRevenueAmt/text()'
            ],
            '990PF': [
                'irs:ReturnData/irs:IRS990PF/irs:TotalRevenueAndExpensesAmt/text()',
                'irs:ReturnData/irs:IRS990PF/irs:AnalysisOfRevenueAndExpenses/irs:TotalRevAndExpnssAmt/text()'
            ],
            '990T': [
                'irs:ReturnData/irs:IRS990T/irs:TotalUBTIAmt/text()'
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
                'irs:ReturnData/irs:IRS990/irs:TotalFunctionalExpensesGrp/irs:TotalAmt/text()',
                'irs:ReturnData/irs:IRS990/irs:StatementOfFunctionalExpenses/irs:TotalFunctionalExpensesAmt/text()',
                'irs:ReturnData/irs:IRS990/irs:StatementOfFunctionalExpenses/irs:TotalAmt/text()',
            ],
            '990EZ': [
                'irs:ReturnData/irs:IRS990EZ/irs:TotalExpensesAmt/text()'
            ],
            '990PF': [
                'irs:ReturnData/irs:IRS990PF/irs:TotalExpensesAndDisbursementsAmt/text()',
                'irs:ReturnData/irs:IRS990PF/irs:AnalysisOfRevenueAndExpenses/irs:TotalExpensesAmt/text()'
            ],
            '990T': [
                'irs:ReturnData/irs:IRS990T/irs:TotalExpensesAmt/text()',
                'irs:ReturnData/irs:IRS990T/irs:TotalDeductionsAmt/text()',
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
                'irs:ReturnData/irs:IRS990/irs:BalanceSheetGrp/irs:TotalAssetsEOYAmt/text()',
                'irs:ReturnData/irs:IRS990/irs:BalanceSheet/irs:TotalAssetsEOYAmt/text()',
            ],
            '990EZ': [
                'irs:ReturnData/irs:IRS990EZ/irs:TotalAssetsEOYAmt/text()',
            ],
            '990PF': [
                'irs:ReturnData/irs:IRS990PF/irs:FMVAssetsEOYAmt/text()',
                'irs:ReturnData/irs:IRS990PF/irs:BalanceSheets/irs:TotalAssetsEOYAmt/text()',
            ],
            '990T': [
                'irs:ReturnData/irs:IRS990T/irs:BalanceSheet/irs:TotalAssetsEOYAmt/text()',
            ],
            'Common': [
                '//*[contains(local-name(), "TotalAssets") and contains(local-name(), "EOY")]/text()',
                '//*[contains(local-name(), "AssetsEOY")]/text()',
            ]
        }
    },
    'MissionStatement': {
        'type': 'string',
        'paths': {
            '990': [
                'irs:ReturnData/irs:IRS990/irs:MissionDesc/text()',
                'irs:ReturnData/irs:IRS990/irs:MissionStatementTxt/text()',
                'irs:ReturnData/irs:IRS990/irs:ActivityOrMissionDesc/text()',
                'irs:ReturnData/irs:IRS990/irs:PrimaryExemptPurposeTxt/text()',
            ],
            '990EZ': [
                'irs:ReturnData/irs:IRS990EZ/irs:PrimaryExemptPurposeTxt/text()',
                'irs:ReturnData/irs:IRS990EZ/irs:MissionDescription/text()',
            ],
            '990PF': [
                'irs:ReturnData/irs:IRS990PF/irs:PrimaryExemptPurposeTxt/text()',
                'irs:ReturnData/irs:IRS990PF/irs:MissionDescription/text()',
            ],
            '990T': [
                'irs:ReturnData/irs:IRS990T/irs:MissionDescription/text()',
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
                'irs:ReturnData/irs:IRS990/irs:BalanceSheetGrp/irs:NetAssetsOrFundBalancesEOYAmt/text()',
                'irs:ReturnData/irs:IRS990/irs:BalanceSheet/irs:NetAssetsOrFundBalancesEOYAmt/text()',
            ],
            '990EZ': [
                'irs:ReturnData/irs:IRS990EZ/irs:NetAssetsOrFundBalancesEOYAmt/text()',
            ],
            '990PF': [
                'irs:ReturnData/irs:IRS990PF/irs:NetAssetsOrFundBalancesEOYAmt/text()',
            ],
            '990T': [
                'irs:ReturnData/irs:IRS990T/irs:BalanceSheet/irs:NetAssetsOrFundBalancesEOYAmt/text()',
            ],
            'Common': [
                '//*[contains(local-name(), "NetAssets") and contains(local-name(), "EOY")]/text()',
                '//*[contains(local-name(), "NetAssetsOrFundBalancesEOYAmt")]/text()',
            ]
        }
    },
    'BusinessActivityCode': {
    'type': 'string',
    'paths': {
        '990': [
            # Existing paths
            'irs:ReturnData/irs:IRS990/irs:PrincipalBusinessActivityCd/text()',
            'irs:ReturnData/irs:IRS990/irs:BusinessActivityCode/text()',
            'irs:ReturnData/irs:IRS990/irs:ProgramServiceAccomplishmentGrp/irs:ActivityCd/text()',
            # Additional paths
            'irs:ReturnData/irs:IRS990/irs:MissionDesc/text()',
            'irs:ReturnData/irs:IRS990/irs:ActivityOrMissionDesc/text()',
            'irs:ReturnData/irs:IRS990/irs:MissionStatementTxt/text()',
            'irs:ReturnData/irs:IRS990/irs:PrimaryExemptPurposeTxt/text()',
        ],
        '990EZ': [
            # Existing paths
            'irs:ReturnData/irs:IRS990EZ/irs:PrincipalBusinessActivityCd/text()',
            'irs:ReturnData/irs:IRS990EZ/irs:BusinessActivityCode/text()',
            'irs:ReturnData/irs:IRS990EZ/irs:ActivityCd/text()',
            # Additional paths
            'irs:ReturnData/irs:IRS990EZ/irs:MissionDescription/text()',
            'irs:ReturnData/irs:IRS990EZ/irs:PrimaryExemptPurposeTxt/text()',
        ],
        '990PF': [
            # Existing paths
            'irs:ReturnData/irs:IRS990PF/irs:PrincipalBusinessActivityCd/text()',
            'irs:ReturnData/irs:IRS990PF/irs:BusinessActivityCode/text()',
            'irs:ReturnData/irs:IRS990PF/irs:ActivityCd/text()',
            # Additional paths
            'irs:ReturnData/irs:IRS990PF/irs:PrimaryExemptPurposeTxt/text()',
        ],
        '990T': [
            # Existing paths
            'irs:ReturnData/irs:IRS990T/irs:PrincipalBusinessActivityCd/text()',
            'irs:ReturnData/irs:IRS990T/irs:BusinessActivityCode/text()',
            # Additional paths
            'irs:ReturnData/irs:IRS990T/irs:MissionDescription/text()',
        ],
        'Common': [
            # Existing paths
            'irs:ReturnHeader/irs:PrincipalBusinessActivityCd/text()',
            'irs:ReturnHeader/irs:BusinessActivityCode/text()',
            '//*[contains(local-name(), "PrincipalBusinessActivityCd")]/text()',
            '//*[contains(local-name(), "BusinessActivityCode")]/text()',
            '//*[contains(local-name(), "ActivityCd")]/text()',
            # Additional paths
            '//*[contains(local-name(), "MissionDesc")]/text()',
            '//*[contains(local-name(), "ActivityOrMissionDesc")]/text()',
            '//*[contains(local-name(), "MissionStatement")]/text()',
            '//*[contains(local-name(), "PrimaryExemptPurposeTxt")]/text()',
        ]
    }
},

}
