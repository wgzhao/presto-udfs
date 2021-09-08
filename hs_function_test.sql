-- udf_add_close_day          | varchar     | varchar, integer
-- udf_add_normal_day         | varchar     | varchar, integer
-- udf_count_close_day        | integer     | varchar, varchar
-- udf_fill_data_by_close_day | varchar     | varchar
-- udf_fill_data_by_close_day | varchar     | varchar, varchar, varchar
-- udf_fill_data_by_close_day | varchar     | varchar, varchar, varchar, varchar, varchar
-- udf_fill_data_by_close_day | varchar     | varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar
-- udf_fill_data_by_close_day | varchar     | varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar
-- udf_hs_date_diff           | integer     | varchar, varchar
-- udf_last_close_day         | varchar     | varchar
-- udf_max_draw_down          | double      | varchar

select udf_add_close_day('20200101', 2);
select udf_add_close_day('20200101', -1);
select udf_add_normal_day('20200101', 10);
select udf_add_normal_day('20200101', -10);
select udf_count_close_day('20200101', '20200301');
select udf_fill_data_by_close_day('20200106');
select udf_hs_date_diff('20200101', '20200302');
select udf_last_close_day('20200607');
select udf_last_close_day('20150105');
select udf_last_close_day('20201231');
select udf_max_draw_down('1,2,3,4,1');

-- test null
select udf_add_close_day(null, 2);
select udf_add_close_day(null, null);
select udf_add_close_day(null, -1);
select udf_add_normal_day(null, 10);
select udf_add_close_day(null, null);
select udf_add_normal_day(null, -10);
select udf_count_close_day(null, '20200301');
select udf_count_close_day(null, null);
select udf_fill_data_by_close_day(null);
select udf_hs_date_diff(null, '20200302');
select udf_hs_date_diff('20200302', null);
select udf_hs_date_diff(null, null);
select udf_last_close_day(null);
select udf_max_draw_down(null);

-- test empty string
select udf_add_close_day('', 2);
select udf_add_close_day('', -1);
select udf_add_normal_day('', 10);
select udf_add_normal_day('', -10);
select udf_count_close_day('', '20200301');
select udf_count_close_day('', '');
select udf_fill_data_by_close_day('');
select udf_hs_date_diff('', '20200302');
select udf_hs_date_diff('', '');
select udf_last_close_day('');
select udf_max_draw_down('');


select udf_fill_data_by_close_day('D!#900947!#20200403!#1!#0.00000000!#97087.0000!#1!#0.287!#0.000!#0!#0.291!#0.283!#0.2870!#0.2870!#20200403!@#D!#900947!#20190425!#1!#0.00000000!#457169.0000!#1!#0.389!#0.000!#0!#0.395!#0.389!#0.3890!#0.3890!#20190425!@#D!#900947!#20200313!#1!#0.00000000!#442754.0000!#1!#0.292!#0.000!#0!#0.294!#0.285!#0.2920!#0.2920!#20200313!@#D!#900947!#20190304!#1!#0.00000000!#2911337.0000!#1!#0.409!#0.000!#0!#0.418!#0.401!#0.4090!#0.4090!#20190304!@#D!#900947!#20180703!#1!#0.00000000!#156746.0000!#1!#0.469!#0.000!#0!#0.470!#0.460!#0.4690!#0.4690!#20180703!@#D!#900947!#20180607!#1!#0.00000000!#209897.0000!#1!#0.501!#0.000!#0!#0.506!#0.500!#0.5010!#0.5010!#20180607!@#D!#900947!#20191230!#1!#0.00000000!#237310.0000!#1!#0.331!#0.000!#0!#0.331!#0.325!#0.3310!#0.3310!#20191230!@#D!#900947!#20200511!#1!#0.00000000!#324944.0000!#1!#0.242!#0.000!#0!#0.250!#0.240!#0.2420!#0.2420!#20200511!@#D!#900947!#20190423!#1!#0.00000000!#570243.0000!#1!#0.392!#0.000!#0!#0.397!#0.392!#0.3920!#0.3920!#20190423!@#D!#900947!#20180605!#1!#0.00000000!#265566.0000!#1!#0.506!#0.000!#0!#0.506!#0.499!#0.5060!#0.5060!#20180605!@#D!#900947!#20190807!#1!#0.00000000!#141769.0000!#1!#0.340!#0.000!#0!#0.343!#0.335!#0.3400!#0.3400!#20190807!@#D!#900947!#20180507!#1!#0.00000000!#251860.0000!#1!#0.517!#0.000!#0!#0.519!#0.511!#0.5170!#0.5170!#20180507!@#D!#900947!#20190625!#1!#0.00000000!#339236.0000!#1!#0.373!#0.000!#0!#0.379!#0.368!#0.3730!#0.3730!#20190625!@#D!#900947!#20191212!#1!#0.00000000!#84370.0000!#1!#0.316!#0.000!#0!#0.316!#0.309!#0.3160!#0.3160!#20191212!@#D!#900947!#20191129!#1!#0.00000000!#335698.0000!#1!#0.311!#0.000!#0!#0.319!#0.306!#0.3110!#0.3110!#20191129!@#D!#900947!#20180612!#1!#0.00000000!#372507.0000!#1!#0.502!#0.000!#0!#0.502!#0.496!#0.5020!#0.5020!#20180612!@#D!#900947!#20200508!#1!#0.00000000!#235923.0000!#1!#0.249!#0.000!#0!#0.250!#0.243!#0.2490!#0.2490!#20200508!@#D!#900947!#20200605!#1!#0.00000000!#101102.0000!#1!#0.208!#0.000!#0!#0.211!#0.208!#0.2080!#0.2080!#20200605',
		'20180301', '20200713','2','14',null,'-1','-1','asc');