-----> stage
-- dimensions
insert into [dbo].[ETLLoads] values 
(1, 'EXTERNAL','DE_LH_100_RAW','Simphony','API','json','getDiscountDimensions','discounts','full','stage',0,NULL,NULL),
(2, 'EXTERNAL','DE_LH_100_RAW','Simphony','API','json','getEmployeeDimensions','employees','full','stage',0,NULL,NULL),
(3, 'EXTERNAL','DE_LH_100_RAW','Simphony','API','json','getLocationDimensions','locations','full','stage',0,NULL,NULL),
(4, 'EXTERNAL','DE_LH_100_RAW','Simphony','API','json','getMenuItemDimensions','menuItems','full','stage',0,NULL,NULL),
(5, 'EXTERNAL','DE_LH_100_RAW','Simphony','API','json','getOrderTypeDimensions','orderTypes','full','stage',0,NULL,NULL),
(6, 'EXTERNAL','DE_LH_100_RAW','Simphony','API','json','getReasonCodeDimensions','reasonCodes','full','stage',0,NULL,NULL),
(7, 'EXTERNAL','DE_LH_100_RAW','Simphony','API','json','getRevenueCenterDimensions','revenueCenters','full','stage',0,NULL,NULL),
(8, 'EXTERNAL','DE_LH_100_RAW','Simphony','API','json','getServiceChargeDimensions','serviceCharges','full','stage',0,NULL,NULL),
(9, 'EXTERNAL','DE_LH_100_RAW','Simphony','API','json','getTaxDimensions','taxes','full','stage',0,NULL,NULL),
(10, 'EXTERNAL','DE_LH_100_RAW','Simphony','API','json','getTenderMediaDimensions','tenderMedias','full','stage',0,NULL,NULL)

-- others (aggregates, facts)
insert into [dbo].[ETLLoads] values 
(31, 'EXTERNAL','DE_LH_100_RAW','Simphony','API','json','getMenuItemDailyTotals','menuItemDailyTotals','incremental','stage',0,NULL,NULL),
(32, 'EXTERNAL','DE_LH_100_RAW','Simphony','API','json','getGuestChecks','guestChecks','incremental','stage',0,NULL,NULL),
--(33, 'EXTERNAL','DE_LH_100_RAW','Simphony','API','json','getKDSDetails','KDSDetails','incremental','stage',1,NULL,NULL)

-----> R2R (flatten)
insert into [dbo].[ETLLoads] values 
(51, 'DE_LH_100_RAW','DE_LH_100_RAW','Simphony','json','table','discounts','discounts','dim','flatt',0,NULL,NULL),
(52, 'DE_LH_100_RAW','DE_LH_100_RAW','Simphony','json','table','employees','employees','dim','flatt',0,NULL,NULL),
(53, 'DE_LH_100_RAW','DE_LH_100_RAW','Simphony','json','table','locations','locations','dim','flatt',0,NULL,NULL),
(54, 'DE_LH_100_RAW','DE_LH_100_RAW','Simphony','json','table','menuItems','menuItems','dim','flatt',0,NULL,NULL),
(55, 'DE_LH_100_RAW','DE_LH_100_RAW','Simphony','json','table','orderTypes','orderTypes','dim','flatt',0,NULL,NULL),
(56, 'DE_LH_100_RAW','DE_LH_100_RAW','Simphony','json','table','reasonCodes','reasonCodes','dim','flatt',0,NULL,NULL),
(57, 'DE_LH_100_RAW','DE_LH_100_RAW','Simphony','json','table','revenueCenters','revenueCenters','dim','flatt',0,NULL,NULL),
(58, 'DE_LH_100_RAW','DE_LH_100_RAW','Simphony','json','table','serviceCharges','serviceCharges','dim','flatt',0,NULL,NULL),
(59, 'DE_LH_100_RAW','DE_LH_100_RAW','Simphony','json','table','taxes','taxes','dim','flatt',0,NULL,NULL),
(60, 'DE_LH_100_RAW','DE_LH_100_RAW','Simphony','json','table','tenderMedias','tenderMedias','dim','flatt',0,NULL,NULL)
-- others (aggregates, facts)
insert into [dbo].[ETLLoads] values 
(81, 'DE_LH_100_RAW','DE_LH_100_RAW','Simphony','json','table','menuItemDailyTotals','menuItemDailyTotals','fact','flatt',0,NULL,NULL),
(82, 'DE_LH_100_RAW','DE_LH_100_RAW','Simphony','json','table','guestChecks','guestChecks','fact','flatt',0,NULL,NULL),
--(83, 'DE_LH_100_RAW','DE_LH_100_RAW','Simphony','json','table','KDSDetails','KDSDetails','fact','flatt',1,NULL,NULL)

----> R2B (dedup)
-- dimensions
insert into [dbo].[ETLLoads] values 
(101, 'DE_LH_100_RAW','DE_LH_200_BASE','Simphony','table','table','discounts','discount','dim','dedup',0,NULL,NULL),
(102, 'DE_LH_100_RAW','DE_LH_200_BASE','Simphony','table','table','employees','employee','dim','dedup',0,NULL,NULL),
(103, 'DE_LH_100_RAW','DE_LH_200_BASE','Simphony','table','table','locations','location','dim','dedup',0,NULL,NULL),
(104, 'DE_LH_100_RAW','DE_LH_200_BASE','Simphony','table','table','locations','workstation','dim','dedup',0,NULL,NULL),
(105, 'DE_LH_100_RAW','DE_LH_200_BASE','Simphony','table','table','menuItems','item','dim','dedup',0,NULL,NULL),
(106, 'DE_LH_100_RAW','DE_LH_200_BASE','Simphony','table','table','orderTypes','order_type','dim','dedup',0,NULL,NULL),
(107, 'DE_LH_100_RAW','DE_LH_200_BASE','Simphony','table','table','reasonCodes','reason','dim','dedup',0,NULL,NULL),
(108, 'DE_LH_100_RAW','DE_LH_200_BASE','Simphony','table','table','revenueCenters','revenue_center','dim','dedup',0,NULL,NULL),
(109, 'DE_LH_100_RAW','DE_LH_200_BASE','Simphony','table','table','serviceCharges','service_charge','dim','dedup',0,NULL,NULL),
(110, 'DE_LH_100_RAW','DE_LH_200_BASE','Simphony','table','table','taxes','tax','dim','dedup',0,NULL,NULL),
(111, 'DE_LH_100_RAW','DE_LH_200_BASE','Simphony','table','table','tenderMedias','tender','dim','dedup',0,NULL,NULL)
-- aggregates, facts
insert into [dbo].[ETLLoads] values 
(131, 'DE_LH_100_RAW','DE_LH_200_BASE','Simphony','table','table','menuItemDailyTotals','menu_item_daily_totals','fact','dedup',0,NULL,NULL),
(132, 'DE_LH_100_RAW','DE_LH_200_BASE','Simphony','table','table','guestChecks','guest_checks','fact','dedup',0,NULL,NULL),
--(133, 'DE_LH_100_RAW','DE_LH_200_BASE','Simphony','table','table','KDSDetails','kds_details','fact','dedup',1,NULL,NULL)

-- sharepoint lists
insert into [dbo].[ETLLoads] values 
(151, 'DE_LH_100_RAW','DE_LH_200_BASE','Sharepoint','table','table','sharepoint_DimSiteSupplement','sharepoint_DimSiteSupplement','sharepoint','dedup',0,NULL,NULL),
(152, 'DE_LH_100_RAW','DE_LH_200_BASE','Sharepoint','table','table','sharepoint_RegionalManagers','sharepoint_RegionalManagers','sharepoint','dedup',0,NULL,NULL),
(153, 'DE_LH_100_RAW','DE_LH_200_BASE','Sharepoint','table','table','sharepoint_ProductMixCategories','sharepoint_ProductMixCategories','sharepoint','dedup',0,NULL,NULL)

----> B2I
insert into [dbo].[ETLLoads] values 
(201, 'DE_LH_200_BASE','DE_LH_300_INTERMEDIATE','Simphony','table','table','inter_prep_cost','inter_prep_cost','intermediate','load',0,NULL,NULL)
--(202, 'DE_LH_200_BASE','DE_LH_300_INTERMEDIATE','Simphony','table','table','inter_prep_time','DELETE_inter_prep_time','intermediate','load',1,NULL,NULL),
--(203, 'DE_LH_200_BASE','DE_LH_300_INTERMEDIATE','Simphony','table','table','gc_pivot_tax','DELETE_inter_gc_pivot_tax','intermediate','load',1,NULL,NULL)

----> B2C
insert into [dbo].[ETLLoads] values 
(251, 'DE_LH_200_BASE','DE_LH_400_CURATED','Simphony','table','table','discount','dim_discount','dim','load',0,NULL,NULL),
(252, 'DE_LH_200_BASE','DE_LH_400_CURATED','Simphony','table','table','employee','dim_employee','dim','load',0,NULL,NULL),
(253, 'DE_LH_200_BASE','DE_LH_400_CURATED','Simphony','table','table','item','dim_item','dim','load',0,NULL,NULL),
(254, 'DE_LH_200_BASE','DE_LH_400_CURATED','Simphony','table','table','location','dim_location','dim','load',0,NULL,NULL),
(255, 'DE_LH_200_BASE','DE_LH_400_CURATED','Simphony','table','table','order_type','dim_order_type','dim','load',0,NULL,NULL),
(256, 'DE_LH_200_BASE','DE_LH_400_CURATED','Simphony','table','table','revenue_center','dim_revenue_center','dim','load',0,NULL,NULL),

(281, 'DE_LH_200_BASE','DE_LH_400_CURATED','Simphony','table','table','fact_sales','fact_sales','fact','load',0,NULL,NULL),
(282, 'DE_LH_200_BASE','DE_LH_400_CURATED','Simphony','table','table','fact_discounts','fact_discounts','fact','load',0,NULL,NULL),
(283, 'DE_LH_200_BASE','DE_LH_400_CURATED','Simphony','table','table','fact_tenders','fact_tenders','fact','load',0,NULL,NULL),
(284, 'DE_LH_200_BASE','DE_LH_400_CURATED','Simphony','table','table','fact_error_correct','fact_error_correct','fact','load',0,NULL,NULL)

--(281, 'DE_LH_200_BASE','DE_LH_400_CURATED','Simphony','table','table','fact_transaction_details','fact_transaction_details','fact','load',1,NULL,NULL),
--(282, 'DE_LH_400_CURATED','DE_LH_400_CURATED','Simphony','table','table','fact_transaction_summary','fact_transaction_summary','fact','load',1,NULL,NULL),
--(283, 'DE_LH_200_BASE','DE_LH_400_CURATED','Simphony','table','table','kds_details','speed_of_service','fact','load',1,NULL,NULL),
--(284, 'DE_LH_200_BASE','DE_LH_400_CURATED','Simphony','table','table','fact_orders','fact_orders','fact','load',1,NULL,NULL),
--(289, 'DE_LH_200_BASE','DE_LH_400_CURATED','Simphony','table','table','fact_others','fact_others','fact1','load',1,NULL,NULL)
