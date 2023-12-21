/*
 * FledgePower HNZ <-> pivot filter utility functions.
 *
 * Copyright (c) 2022, RTE (https://www.rte-france.com)
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Michael Zillgith (michael.zillgith at mz-automation.de)
 * 
 */

#include <sstream>
#include "hnz_pivot_utility.hpp"

std::string HnzPivotUtility::join(const std::vector<std::string> &list, const std::string &sep /*= ", "*/)
{
    std::string ret;
    for(const auto &str : list) {
        if(!ret.empty()) {
            ret += sep;
        }
        ret += str;
    }
    return ret;
}

std::vector<std::string> HnzPivotUtility::split(const std::string& str, char sep) {
    std::stringstream ss(str);
    std::string item;
    std::vector<std::string> elems;
    while (std::getline(ss, item, sep)) {
        elems.push_back(std::move(item));
    }
    if (elems.empty()) {
        elems.push_back(str);
    }
    return elems;
}
