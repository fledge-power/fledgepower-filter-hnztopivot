#include <gtest/gtest.h>

#include "hnz_pivot_utility.hpp"

TEST(PivotHNZPluginUtility, Join)
{
    ASSERT_STREQ(HnzPivotUtility::join({}).c_str(), "");
    ASSERT_STREQ(HnzPivotUtility::join({"TEST"}).c_str(), "TEST");
    ASSERT_STREQ(HnzPivotUtility::join({"TEST", "TOAST", "TASTE"}).c_str(), "TEST, TOAST, TASTE");
    ASSERT_STREQ(HnzPivotUtility::join({"TEST", "", "TORTOISE"}).c_str(), "TEST, , TORTOISE");
    ASSERT_STREQ(HnzPivotUtility::join({}, "-").c_str(), "");
    ASSERT_STREQ(HnzPivotUtility::join({"TEST"}, "-").c_str(), "TEST");
    ASSERT_STREQ(HnzPivotUtility::join({"TEST", "TOAST", "TASTE"}, "-").c_str(), "TEST-TOAST-TASTE");
    ASSERT_STREQ(HnzPivotUtility::join({"TEST", "", "TORTOISE"}, "-").c_str(), "TEST--TORTOISE");
    ASSERT_STREQ(HnzPivotUtility::join({}, "").c_str(), "");
    ASSERT_STREQ(HnzPivotUtility::join({"TEST"}, "").c_str(), "TEST");
    ASSERT_STREQ(HnzPivotUtility::join({"TEST", "TOAST", "TASTE"}, "").c_str(), "TESTTOASTTASTE");
    ASSERT_STREQ(HnzPivotUtility::join({"TEST", "", "TORTOISE"}, "").c_str(), "TESTTORTOISE");
}

TEST(PivotHNZPluginUtility, Split)
{
    ASSERT_EQ(HnzPivotUtility::split("", '-'), std::vector<std::string>{""});
    ASSERT_EQ(HnzPivotUtility::split("TEST", '-'), std::vector<std::string>{"TEST"});
    std::vector<std::string> out1{"TEST", "TOAST", "TASTE"};
    ASSERT_EQ(HnzPivotUtility::split("TEST-TOAST-TASTE", '-'), out1);
    std::vector<std::string> out2{"TEST", "", "TORTOISE"};
    ASSERT_EQ(HnzPivotUtility::split("TEST--TORTOISE", '-'), out2);
}

TEST(PivotHNZPluginUtility, Logs)
{
    std::string text("This message is at level %s");
    ASSERT_NO_THROW(HnzPivotUtility::log_debug(text.c_str(), "debug"));
    ASSERT_NO_THROW(HnzPivotUtility::log_info(text.c_str(), "info"));
    ASSERT_NO_THROW(HnzPivotUtility::log_warn(text.c_str(), "warning"));
    ASSERT_NO_THROW(HnzPivotUtility::log_error(text.c_str(), "error"));
    ASSERT_NO_THROW(HnzPivotUtility::log_fatal(text.c_str(), "fatal"));
}