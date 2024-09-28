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
        'type': 'int',
        'paths': {
            'Common': [
                'irs:ReturnHeader/irs:Filer/irs:EIN/text()'
            ]
        }
    },
    'TaxYr': {
        'type': 'int',
        'paths': {
            'Common': [
                'irs:ReturnHeader/irs:TaxYr/text()',
                'irs:ReturnHeader/irs:TaxYear/text()',
                'substring(irs:ReturnHeader/irs:TaxPeriodEndDt/text(),1,4)'
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
                'irs:ReturnData/irs:IRS990/irs:Revenue/irs:TotalRevenueAmt/text()',
                'irs:ReturnData/irs:IRS990/irs:TotalRevenueGrp/irs:TotalRevenueColumnAmt/text()'
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
}
,
    
    
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
}

}

