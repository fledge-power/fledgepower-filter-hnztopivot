#include <gtest/gtest.h>
#include <plugin_api.h>
#include <rapidjson/document.h>

#include "version.h"

using namespace rapidjson;

extern "C" {
	PLUGIN_INFORMATION *plugin_info(); 
};

TEST(PivotHNZPluginInfo, PluginInfo)
{
	PLUGIN_INFORMATION *info = plugin_info();
	ASSERT_STREQ(info->name, "hnz_pivot_filter");
	ASSERT_STREQ(info->version, VERSION);
	ASSERT_EQ(info->options, 0);
	ASSERT_STREQ(info->type, PLUGIN_TYPE_FILTER);
	ASSERT_STREQ(info->interface, "1.0.0");
}

TEST(PivotHNZPluginInfo, PluginInfoConfigParse)
{
	PLUGIN_INFORMATION *info = plugin_info();
	Document doc;
	doc.Parse(info->config);
	ASSERT_EQ(doc.HasParseError(), false);
	ASSERT_EQ(doc.IsObject(), true);
	ASSERT_EQ(doc.HasMember("plugin"), true);
	ASSERT_EQ(doc.HasMember("enable"), true);
	ASSERT_EQ(doc.HasMember("exchanged_data"), true);
}
