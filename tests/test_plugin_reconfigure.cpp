#include <gtest/gtest.h>

#include "hnz_pivot_filter.hpp"


static std::string reconfigure = QUOTE({
    "enable": {
        "value": "false"
    }
});

extern "C" {
	PLUGIN_INFORMATION *plugin_info();
	PLUGIN_HANDLE plugin_init(ConfigCategory *config,
			  OUTPUT_HANDLE *outHandle,
			  OUTPUT_STREAM output);

    void plugin_reconfigure(PLUGIN_HANDLE *handle, const std::string& newConfig);
    void plugin_shutdown(PLUGIN_HANDLE *handle);
};

TEST(PivotHNZPluginReconfigure, Reconfigure) 
{
	PLUGIN_INFORMATION *info = plugin_info();
    ConfigCategory *config = new ConfigCategory("hnztopivot", info->config);
    
    ASSERT_NE(config, nullptr);		
    config->setItemsValueFromDefault();
    config->setValue("enable", "true");
    
    PLUGIN_HANDLE handle = nullptr;
    ASSERT_NO_THROW(handle = plugin_init(config, nullptr, nullptr));
    HNZPivotFilter* filter = static_cast<HNZPivotFilter*>(handle);
    
    ASSERT_NO_THROW(plugin_reconfigure(static_cast<PLUGIN_HANDLE*>(handle), reconfigure));
    ASSERT_EQ(filter->isEnabled(), false);

    ASSERT_NO_THROW(plugin_shutdown(static_cast<PLUGIN_HANDLE*>(handle)));
}