#include <gtest/gtest.h>
#include <plugin_api.h>
#include <datapoint.h>
#include "hnz_pivot_object.hpp"
#include <memory>


Datapoint * getDatapoint(const std::string & name, long value)
{
	DatapointValue vl(value); 
	return new Datapoint(name,vl);
}

Datapoint * getDatapoint(const std::string & name, const std::string & value)
{
	DatapointValue vl(value); 
	return new Datapoint(name,vl);
}

class HnzPivotObjectTest
{
	public:
	HnzPivotObjectTest(){
		po = new HnzPivotObject("GTIS", "no");
	}
	~HnzPivotObjectTest(){
		delete po;
	}

    void handleCdcSps(Datapoint* cdc)  { po->handleCdcSps(cdc); }

	void handleCdcMv(Datapoint* cdc)  { po->handleCdcMv(cdc); }
	void handleCdcT(Datapoint* cdc)  { po->handleCdcT(cdc); }

	void handleCdcQ(Datapoint* cdc)  { po->handleCdcQ(cdc); }
	void handleCdc(Datapoint* cdc)  { po->handleCdc(cdc); }

	void handleCdcDpc(Datapoint* cdc) { po->handleCdcDpc(cdc); }

	void setCscClass(HnzPivotObject::HnzPivotCdc cs) {
		po->m_pivotCdc = cs;
	}
	
private:
    HnzPivotObject *po;
};


TEST(PivotHNZPluginObject, HNZPivotDataPointConstructorBadPivotName)
{
	DatapointValue vl("NO_PIVOT"); 
	auto dt = std::unique_ptr<Datapoint>(new Datapoint("NO_PIVOT",vl));
	EXPECT_THROW(HnzPivotObject(dt.get()),HnzPivotObjectException);
}

TEST(PivotHNZPluginObject, HNZPivotDataPointConstructorBadPivotValue)
{
	DatapointValue vl("PIVOT"); 
	auto dt = std::unique_ptr<Datapoint>(new Datapoint("PIVOT",vl));
	EXPECT_THROW(HnzPivotObject(dt.get()),HnzPivotObjectException);
}

TEST(PivotHNZPluginObject, HNZPivotDataPointCdcSps)
{
	HnzPivotObjectTest pvt;
	EXPECT_NO_THROW(pvt.handleCdcSps(getDatapoint("ctlVal", 10L)));

	EXPECT_NO_THROW(pvt.handleCdcSps(nullptr));
	std::vector<Datapoint*> * vdt = new std::vector<Datapoint*>();
	vdt->push_back(getDatapoint("ctlVal", 10L));
	DatapointValue vbl(vdt, true); 
	auto dt = new Datapoint("PIVOT",vbl);
	EXPECT_NO_THROW(pvt.handleCdcSps(dt));
}

TEST(PivotHNZPluginObject, HNZPivotDataPointCdcMv)
{
	HnzPivotObjectTest pvt;
	EXPECT_THROW(pvt.handleCdcMv(getDatapoint("Mag", 10L)),HnzPivotObjectException);

	EXPECT_THROW(pvt.handleCdcMv(nullptr),HnzPivotObjectException);
	std::vector<Datapoint*> * mag = new std::vector<Datapoint*>();
	std::vector<Datapoint*> * i = new std::vector<Datapoint*>();
	EXPECT_THROW(pvt.handleCdcMv(getDatapoint("mag", 10L)),HnzPivotObjectException);

	i->push_back(getDatapoint("i", 10L));
	DatapointValue vbl(mag, true); 
	DatapointValue dvi(i, true); 
	mag->push_back(new Datapoint("mag", dvi));
	auto dt = new Datapoint("PIVOT",vbl);
	EXPECT_NO_THROW(pvt.handleCdcMv(dt));

}

TEST(PivotHNZPluginObject, HNZPivotDataPointT)
{
	HnzPivotObjectTest pvt;
	std::vector<Datapoint*> * cdcDatapoint = new std::vector<Datapoint*>();
	cdcDatapoint->push_back(getDatapoint("t", 10L));
	DatapointValue cdcValue(cdcDatapoint, true); 
	auto cdc = new Datapoint("cdc",cdcValue);
	EXPECT_NO_THROW(pvt.handleCdcT(nullptr));
	EXPECT_NO_THROW(pvt.handleCdcT(cdc));

	cdcDatapoint->clear();
	cdcDatapoint->push_back(getDatapoint("a", 10L));
	EXPECT_NO_THROW(pvt.handleCdcT(cdc));
}


TEST(PivotHNZPluginObject, HNZPivotDataPointQ)
{
	HnzPivotObjectTest pvt;
	std::vector<Datapoint*> * cdcDatapoint = new std::vector<Datapoint*>();
	cdcDatapoint->push_back(getDatapoint("q", 10L));
	DatapointValue cdcValue(cdcDatapoint, true); 
	auto cdc = new Datapoint("cdc",cdcValue);
	EXPECT_NO_THROW(pvt.handleCdcQ(nullptr));
	EXPECT_NO_THROW(pvt.handleCdcQ(cdc));

}

TEST(PivotHNZPluginObject, HNZPivotDataPointQBadKey)
{
	HnzPivotObjectTest pvt;
	std::vector<Datapoint*> * cdcDatapoint = new std::vector<Datapoint*>();
	cdcDatapoint->push_back(getDatapoint("a", 10L));
	DatapointValue cdcValue(cdcDatapoint, true); 
	auto cdc = new Datapoint("cdc",cdcValue);
	EXPECT_NO_THROW(pvt.handleCdcQ(cdc));
}

TEST(PivotHNZPluginObject, HNZPivotDataPointQBadValue)
{
	HnzPivotObjectTest pvt;
	std::vector<Datapoint*> * cdcDatapoint = new std::vector<Datapoint*>();
	cdcDatapoint->push_back(getDatapoint("a", "test"));
	DatapointValue cdcValue(cdcDatapoint, true); 
	auto cdc = new Datapoint("cdc",cdcValue);
	EXPECT_NO_THROW(pvt.handleCdcQ(cdc));
}

TEST(PivotHNZPluginObject, HNZPivotDataPointCdc)
{
	HnzPivotObjectTest pvt;
	EXPECT_THROW(pvt.handleCdc(nullptr),HnzPivotObjectException);

	pvt.setCscClass(HnzPivotObject::HnzPivotCdc::DPS);

	std::vector<Datapoint*> * cdcDatapoint = new std::vector<Datapoint*>();
	cdcDatapoint->push_back(getDatapoint("q", 10L));
	DatapointValue cdcValue(cdcDatapoint, true); 
	auto cdc = new Datapoint("cdc",cdcValue);

	EXPECT_NO_THROW(pvt.handleCdc(cdc));
}

TEST(PivotHNZPluginObject, HNZPivotDataPointDpcNull)
{
	HnzPivotObjectTest pvt;
	EXPECT_NO_THROW(pvt.handleCdcDpc(nullptr));
}

TEST(PivotHNZPluginObject, HNZPivotDataPointDpcOn)
{
	HnzPivotObjectTest pvt;
	std::vector<Datapoint*> * cdcDatapoint = new std::vector<Datapoint*>();
	cdcDatapoint->push_back(getDatapoint("ctlVal", "on"));
	DatapointValue cdcValue(cdcDatapoint, true); 
	auto cdc = new Datapoint("cdc",cdcValue);

	EXPECT_NO_THROW(pvt.handleCdcDpc(cdc));
}

TEST(PivotHNZPluginObject, HNZPivotDataPointDpcOff)
{
	HnzPivotObjectTest pvt;
	std::vector<Datapoint*> * cdcDatapoint = new std::vector<Datapoint*>();
	cdcDatapoint->push_back(getDatapoint("ctlVal", "off"));
	DatapointValue cdcValue(cdcDatapoint, true); 
	auto cdc = new Datapoint("cdc",cdcValue);

	EXPECT_NO_THROW(pvt.handleCdcDpc(cdc));
}

TEST(PivotHNZPluginObject, HNZPivotDataPointDpcThrow)
{
	HnzPivotObjectTest pvt;
	std::vector<Datapoint*> * cdcDatapoint = new std::vector<Datapoint*>();
	cdcDatapoint->push_back(getDatapoint("ctlVal", "throw"));
	DatapointValue cdcValue(cdcDatapoint, true); 
	auto cdc = new Datapoint("cdc",cdcValue);

	EXPECT_THROW(pvt.handleCdcDpc(cdc),HnzPivotObjectException);
}

TEST(PivotHNZPluginObject, HNZPivotDataPointDpcBadValue)
{
	HnzPivotObjectTest pvt;
	std::vector<Datapoint*> * cdcDatapoint = new std::vector<Datapoint*>();
	cdcDatapoint->push_back(getDatapoint("ctlVal", 10));
	DatapointValue cdcValue(cdcDatapoint, true); 
	auto cdc = new Datapoint("cdc",cdcValue);

	EXPECT_THROW(pvt.handleCdcDpc(cdc),HnzPivotObjectException);
}