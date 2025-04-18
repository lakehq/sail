-- SQLBench-DS query 98 derived from TPC-DS query 98 under the terms of the TPC Fair Use Policy.
-- TPC-DS queries are Copyright 2021 Transaction Processing Performance Council.
-- This query was generated at scale factor 1.
select
  i_item_id,
  i_item_desc,
  i_category,
  i_class,
  i_current_price,
  sum(ss_ext_sales_price) as itemrevenue,
  sum(ss_ext_sales_price) * 100 / sum(sum(ss_ext_sales_price)) over (
    partition by
      i_class
  ) as revenueratio
from
  store_sales,
  item,
  date_dim
where
  ss_item_sk = i_item_sk
  and i_category in ('Shoes', 'Music', 'Men')
  and ss_sold_date_sk = d_date_sk
  and d_date between cast('2000-01-05' as date) and (cast('2000-01-05' as date) + INTERVAL '30 DAYS')
group by
  i_item_id,
  i_item_desc,
  i_category,
  i_class,
  i_current_price
order by
  i_category,
  i_class,
  i_item_id,
  i_item_desc,
  revenueratio;
