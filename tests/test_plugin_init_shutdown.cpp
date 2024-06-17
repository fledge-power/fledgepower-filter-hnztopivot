#include <gtest/gtest.h>

#include "hnz_pivot_filter.hpp"

extern "C" {
	PLUGIN_INFORMATION *plugin_info();
	PLUGIN_HANDLE plugin_init(ConfigCategory *config,
			  OUTPUT_HANDLE *outHandle,
			  OUTPUT_STREAM output);

    void plugin_shutdown(PLUGIN_HANDLE *handle);
};

TEST(PivotHNZPluginInitShutdown, PluginInitNoConfig)
{
    PLUGIN_HANDLE handle = nullptr;
	ASSERT_NO_THROW(handle = plugin_init(nullptr, nullptr, nullptr));
    ASSERT_TRUE(handle != nullptr);

	HNZPivotFilter* filter = static_cast<HNZPivotFilter*>(handle);
	ASSERT_EQ(filter->isEnabled(), true);

    ASSERT_NO_THROW(plugin_shutdown(static_cast<PLUGIN_HANDLE*>(handle)));
}

TEST(PivotHNZPluginInitShutdown, PluginShutdown) 
{
	PLUGIN_INFORMATION *info = plugin_info();
	ConfigCategory *config = new ConfigCategory("hnztopivot", info->config);
	
	ASSERT_NE(config, nullptr);		
	config->setItemsValueFromDefault();
	
	PLUGIN_HANDLE handle = nullptr;
	ASSERT_NO_THROW(handle = plugin_init(config, nullptr, nullptr));
	ASSERT_TRUE(handle != nullptr);

	HNZPivotFilter* filter = static_cast<HNZPivotFilter*>(handle);
	ASSERT_EQ(filter->isEnabled(), true);

	ASSERT_NO_THROW(plugin_shutdown(static_cast<PLUGIN_HANDLE*>(handle)));
}