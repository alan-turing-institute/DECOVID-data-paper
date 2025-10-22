insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(18, 'impossible measurement value', 'numerical measurement value falls outside of possible range specified by clinicians', 6)
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 1023094
and unit_concept_id = 8753
and value_as_number not between -30 and  30;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3000285
and unit_concept_id = 8749
and value_as_number not between 110 and  180;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3000483
and unit_concept_id = 8753
and value_as_number not between 0 and  60;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3000620
and unit_concept_id = 8636
and value_as_number not between 0 and  5;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3000963
and unit_concept_id = 8636
and value_as_number not between 0 and  250;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3001110
and unit_concept_id = 8923
and value_as_number not between 0 and  5000;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3002385
and unit_concept_id = 8554
and value_as_number not between 0 and  80;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3002400
and unit_concept_id = 8636
and value_as_number not between 0 and  178;  -- Updated to new limits for umol 20201014


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3003338
and unit_concept_id = 8636
and value_as_number not between 0 and  1000;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3004789
and unit_concept_id = 8636
and value_as_number not between 0 and  30;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3005456
and unit_concept_id = 8753
and value_as_number not between 0 and  10;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3007194
and value_as_number not between 2 and  15;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3007220
and unit_concept_id = 8923
and value_as_number not between 0 and  800000;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3007461
and unit_concept_id = 44777588
and value_as_number not between 0 and  2500;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3007930
and unit_concept_id = 8554
and value_as_number not between 0 and  40;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3008223
and value_as_number not between 1 and  6;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3009094
and value_as_number not between 1 and  5;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3009542
and unit_concept_id = 44777604
and value_as_number not between 0 and  1;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3010421
and unit_concept_id = 8482
and value_as_number not between 6.75 and  7.75;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3010813
and unit_concept_id = 44777588
and value_as_number not between 0 and  100;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3011152
and unit_concept_id = 8845
and value_as_number < 0;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3011689
and unit_concept_id = 8636
and value_as_number not between 0 and  260;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3011961
and unit_concept_id = 8842
and value_as_number < 0;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3013176
and value_as_number not between 0 and  6;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3013290
and unit_concept_id = 44777602
and value_as_number not between 0 and  30;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3013429
and unit_concept_id = 44777588
and value_as_number not between 0 and  30;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3013466
and unit_concept_id = 8555
and value_as_number not between 0 and  200;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3013502
and unit_concept_id = 8554
and value_as_number not between 0 and  100;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3013721
and unit_concept_id = 8923
and value_as_number not between 0 and  5000;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3014835
and unit_concept_id = 8519
and value_as_number not between 0 and  10;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3015183
and unit_concept_id = 8752
and value_as_number not between 0 and  250;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3015377
and unit_concept_id = 8753
and value_as_number not between 0 and  5;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3016293
and unit_concept_id = 8753
and value_as_number not between 0 and  80;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3016335
and value_as_number not between 1 and  4;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3016407
and unit_concept_id = 8636
and value_as_number not between 0 and  15;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3017766
and unit_concept_id = 8636
and value_as_number not between 0 and  2;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3018572
and unit_concept_id = 8753
and value_as_number not between 60 and  160;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3018913
and unit_concept_id = 8753
and value_as_number not between 0 and  8;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3020059
and unit_concept_id = 8753
and value_as_number not between 0 and  5;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3020149
and unit_concept_id = 8736
and value_as_number not between 0 and  500;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3020460
and unit_concept_id = 8751
and value_as_number not between 0 and  1000;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3020564
and unit_concept_id = 8749
and value_as_number not between 0 and  2000;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3020716
and unit_concept_id = 8523
and value_as_number not between 0.21 and  1;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3020779
and unit_concept_id = 8753
and value_as_number not between 0 and  80;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3021044
and unit_concept_id = 8636
and value_as_number not between 0 and  178;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3021119
and unit_concept_id = 8753
and value_as_number not between 0 and  3;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3023520
and value_as_number not between 0 and  1000;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3024561
and unit_concept_id = 8636
and value_as_number not between 0 and  70;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3024731
and unit_concept_id = 8583
and value_as_number not between 0 and  150;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3025315
and unit_concept_id = 9529
and value_as_number not between 0 and  300;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3025839
and unit_concept_id = 8753
and value_as_number not between 0 and  60;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3026361
and unit_concept_id = 8734
and value_as_number not between 0 and  10;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3026493
and value_as_number not between 0 and  1200;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3026910
and unit_concept_id = 8923
and value_as_number not between 0 and  10000;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3027315
and unit_concept_id = 44777602
and value_as_number not between 0 and  100;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3027945
and unit_concept_id = 8554
and value_as_number not between 0 and  60;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3028615
and unit_concept_id = 44777588
and value_as_number not between 0 and  20;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3029435
and unit_concept_id = 8725
and value_as_number not between 0 and  100000;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3032080
and unit_concept_id = 8555
and value_as_number not between 0 and  12;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3033836
and unit_concept_id = 8753
and value_as_number not between 0 and  6;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3034426
and unit_concept_id = 8555
and value_as_number not between 0 and  150;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3035941
and unit_concept_id = 8564
and value_as_number not between 0 and  85;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3036277
and unit_concept_id = 9546
and value_as_number not between 0 and  2.72;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3041084
and unit_concept_id = 44777588
and value_as_number not between 0 and  25;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3047181
and unit_concept_id = 8753
and value_as_number not between 0 and  30;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 3048529
and unit_concept_id = 8725
and value_as_number not between 0 and  10000;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4009426
and unit_concept_id = 8753
and value_as_number not between 0 and  10;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4011133
and unit_concept_id = 8753
and value_as_number not between 0 and  10;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4087260
and value_as_number not between 0 and  1000;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4090193
and unit_concept_id = 8519
and value_as_number not between 0 and  30;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4092501
and unit_concept_id = 8519
and value_as_number not between 0 and  30;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4108138
and unit_concept_id = 8541
and value_as_number not between 0 and  100;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4108450
and value_as_number not between 0 and  20;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4141684
and unit_concept_id = 8698
and value_as_number not between 0 and  15;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4152194
and unit_concept_id = 8876
and value_as_number not between 0 and  300;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4154772
and unit_concept_id = 8541
and value_as_number not between 0 and  100;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4154790
and unit_concept_id = 8876
and value_as_number not between 0 and  200;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4176265
and value_as_number not between 0 and  6;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4177991
and value_as_number not between 1 and  7;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4199405
and unit_concept_id = 8753
and value_as_number not between 0 and  25;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4208972
and unit_concept_id = 8554
and value_as_number < 0;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4215838
and unit_concept_id = 4118138
and value_as_number not between 0 and  80;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4216746
and unit_concept_id = 4118138
and value_as_number not between 0 and  80;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4220163
and unit_concept_id = 8587
and value_as_number not between 0 and  6000;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4221102
and unit_concept_id = 8698
and value_as_number not between 0 and  35;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4239021
and unit_concept_id = 8876
and value_as_number not between 0 and  300;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4239408
and unit_concept_id = 8541
and value_as_number not between 0 and  350;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4241837
and value_as_number not between 0 and  8;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4302666
and unit_concept_id = 586323
and value_as_number not between 10 and  45;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4313591
and unit_concept_id = 8541
and value_as_number not between 0 and  120;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4321758
and unit_concept_id = 8554
and value_as_number not between 0 and  100;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4353621
and unit_concept_id = 8519
and value_as_number not between 0 and  160;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 4353938
and unit_concept_id = 8554
and value_as_number not between 0 and  16;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 21490675
and unit_concept_id = 4118138
and value_as_number not between 0 and  50;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 36684829
and value_as_number not between -5 and  4;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 37208354
and value_as_number not between 0 and  21;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 37208374
and value_as_number not between 0 and  3;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 37208375
and value_as_number not between 0 and  3;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 37208376
and value_as_number not between 0 and  3;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 37208377
and value_as_number not between 0 and  3;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 37208378
and value_as_number not between 0 and  3;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 37208379
and value_as_number not between 0 and  3;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 37208380
and value_as_number not between 0 and  3;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 37208381
and value_as_number not between 0 and  3;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 37392971
and unit_concept_id = 8842
and value_as_number not between 0 and  100;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 40483383
and value_as_number not between 1 and  9;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 40757494
and unit_concept_id = 8749
and value_as_number not between 0 and  1000;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 40762499
and unit_concept_id = 8554
and value_as_number not between 0 and  100;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 40776165
and unit_concept_id = 8554
and value_as_number not between 0 and  25;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 40785907
and unit_concept_id = 8842
and value_as_number not between 0 and  100000;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 42869452
and unit_concept_id = 8554
and value_as_number not between 0 and  100;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 46235106
and unit_concept_id = 8923
and value_as_number not between 0 and  2500;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 46235169
and value_as_number not between 0 and  6000;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 46235370
and unit_concept_id = 8923
and value_as_number not between 0 and  15000;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_number', issue_id
from measurement
join issue on issue.short_name = 'impossible measurement value'
where measurement_concept_id = 46236952
and unit_concept_id = 9117
and value_as_number not between 0 and  90;


