#include <gtest/gtest.h>
#include <plugin_api.h>

#include "hnz_pivot_filter_config.hpp"

TEST(PivotHNZPluginConfig, PivotConfigValid)
{
	HNZPivotConfig testConfig;
	testConfig.importExchangeConfig(QUOTE({
		"exchanged_data" : {
			"name" : "SAMPLE",
			"version" : "1.0",
			"datapoints" : [
				{
					"label" : "TS1",
					"pivot_id" : "ID114562",
					"pivot_type" : "SpsTyp",
					"protocols" : [
						{
							"name" : "iec104",
							"address" : "45-672",
							"typeid" : "M_SP_TB_1"
						},
						{
							"name" : "tase2",
							"address" : "S_114562",
							"typeid" : "Data_StateQTimeTagExtended"
						},
						{
							"name" : "hnzip",
							"address" : "511",
							"typeid" : "TS"
						}
					]
				},
				{
					"label" : "TM1",
					"pivot_id" : "ID99876",
					"pivot_type" : "MvTyp",
					"protocols" : [
						{
							"name" : "iec104",
							"address" : "45-984",
							"typeid" : "M_ME_NA_1"
						},
						{
							"name" : "tase2",
							"address" : "S_114563",
							"typeid" : "Data_RealQ"
						},
						{
							"name" : "hnzip",
							"address" : "512",
							"typeid" : "TM"
						}
					]
				}
			]
		}
	}));
	ASSERT_TRUE(testConfig.isComplete());
}

TEST(PivotHNZPluginConfig, PivotConfigInvalidJson)
{
	HNZPivotConfig testConfig;
	testConfig.importExchangeConfig("invalid json config");
	ASSERT_FALSE(testConfig.isComplete());
}

TEST(PivotHNZPluginConfig, PivotConfigMissingRootObject)
{
	HNZPivotConfig testConfig;
	testConfig.importExchangeConfig("{}");
	ASSERT_FALSE(testConfig.isComplete());
}

TEST(PivotHNZPluginConfig, PivotConfigMissingFields)
{
	HNZPivotConfig testConfig;
	testConfig.importExchangeConfig(QUOTE({
		"exchanged_data" : {}
	}));
	ASSERT_FALSE(testConfig.isComplete());
}

TEST(PivotHNZPluginConfig, PivotConfigInvalidFields)
{
	HNZPivotConfig testConfig;
	testConfig.importExchangeConfig(QUOTE({
		"exchanged_data" : {
			"name" : 42,
			"version" : false,
			"datapoints" : [
				{
					"label" : 42,
					"pivot_id" : 42,
					"pivot_type" : 42,
					"protocols" : [
						{
							"name" : 42,
							"address" : "aaa",
							"typeid" : 42
						}
					]
				}
			]
		}
	}));
	ASSERT_FALSE(testConfig.isComplete());
}

TEST(PivotHNZPluginConfig, PivotConfigMsgAddressOutOfRange)
{
	HNZPivotConfig testConfig;
	testConfig.importExchangeConfig(QUOTE({
		"exchanged_data" : {
			"name" : "SAMPLE",
			"version" : "1.0",
			"datapoints" : [
				{
					"label" : "TS1",
					"pivot_id" : "ID114562",
					"pivot_type" : "SpsTyp",
					"protocols" : [
						{
							"name" : "hnzip",
							"address" : "9999999999",
							"typeid" : "TS"
						}
					]
				}
			]
		}
	}));
	ASSERT_FALSE(testConfig.isComplete());
}