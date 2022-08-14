
CREATE TABLE IF NOT EXISTS alimama_kgb_algo_dev.kgb_match_qy_opt03_test_samples_hash_columnized_weighted_neg
(
    sample_id                       STRING --"样本唯一标识，样本id={key1:value1,key2:value2}"
    ,label                          ARRAY<DOUBLE>
    ,unseen_flag                    ARRAY<BIGINT>
    -- ,query_id                       ARRAY<STRING>
    ,g_uid                        ARRAY<STRING> -- g_uid
    ,urb_seq_length                 ARRAY<STRING>
    ,preitem                        ARRAY<STRING> -- preitem
    ,preshop                        ARRAY<STRING> -- preshop
    ,prebrand                       ARRAY<STRING> -- prebrand
    ,precate                        ARRAY<STRING>
    ,prerootcate                    ARRAY<STRING>
    ,precity                        ARRAY<STRING> -- precity
    ,prequery                       ARRAY<STRING>
    ,prepage                        ARRAY<STRING>
    ,item_id                        ARRAY<STRING> -- ad_itemid
    ,clienttype_istop               ARRAY<STRING> -- clienttype_istop
    ,clienttype                     ARRAY<STRING> -- clienttype
    ,istop                          ARRAY<STRING> -- istop
    ,ad_cate_root                   ARRAY<STRING> --'一级类目ID，缺省为-1（-1表示无类目，0表示根类目）'
    ,ad_cate_leaf                   ARRAY<STRING> --'商品的叶子类目ID，缺省为-1（-1表示无类目，0表示根类目）' -- ad_leaf_cateid
    ,ad_brand_id                    ARRAY<STRING> --'商品品牌ID，缺省是0, 注意0也是有意义的，可以理解为非标准品牌或者杂牌' -- ad_brandid
    ,ad_shopid                      ARRAY<STRING> --'商品对应的店铺ID，缺省是0' -- ad_shopid
    ,ad_commodity_id                ARRAY<STRING> --'商品品类ID，缺省是0'
    ,ad_tradeid                     ARRAY<STRING>  -- ad_tradeid
    ,ad_cityid                      ARRAY<STRING>  -- ad_cityid
    ,ad_priceid                     ARRAY<STRING>  -- ad_priceid
    ,ad_b_or_c                      ARRAY<STRING>  -- ad_b_or_c
    ,adgroup_id                     ARRAY<STRING>  -- adgroup_id
    ,upp_age                        ARRAY<STRING> --'用户年龄，缺省为0'-- age
    ,upp_gender                     ARRAY<STRING> --'用户性别，缺省为0'-- gender
    ,upp_occupation                 ARRAY<STRING> --'用户职业，缺省为0'-- occupation
    ,upp_province                   ARRAY<STRING> --'用户省份，缺省为0'-- province
    ,upp_city                       ARRAY<STRING> --'用户城市，缺省为0'-- city
    ,upp_u_star_level               ARRAY<STRING> --'用户星级，缺省为0'-- u_star_level
    ,upp_u_perfer_cates             ARRAY<STRING> --'用户喜欢的类目列表，缺省为空串' -- u_perfer_cates
    ,upp_u_perfer_brandids          ARRAY<STRING> --'用户喜欢的品牌列表，缺省为空串' -- u_perfer_brandids
    ,upp_u_purchase_level           ARRAY<STRING> --'用户购买力，缺省为0' -- u_purchase_level
    ,upp_u_user_vip_level           ARRAY<STRING> --'用户VIP等级，缺省为0'
    ,q_qid                          ARRAY<STRING> -- qid
    ,q_cate_prop                    ARRAY<STRING> -- q_cate_prop
    ,q_primary_terms                ARRAY<STRING> -- q_primary_terms
    ,q_terms                        ARRAY<STRING> -- q_terms
    ,q_pcategory_id_list            ARRAY<STRING>
    ,q_term_len                     ARRAY<STRING>
    ,q_unigram_len                  ARRAY<STRING>
    ,q_pcategory_len                ARRAY<STRING>
    ,q_pcateid_level1               ARRAY<STRING> --"rewrite_rankinfo中预测权重最大的一级类目"
) PARTITIONED BY (ds STRING) LIFECYCLE 10; 

-- set odps.instance.priority=6;
-- set odps.sql.type.system.odps2=true;
-- set odps.sql.joiner.instances=2000;
-- set odps.sql.reducer.instances=2000;
INSERT OVERWRITE TABLE alimama_kgb_algo_dev.kgb_match_qy_opt03_test_samples_hash_columnized_weighted_neg PARTITION(ds='${bizdate}')
SELECT sample_id
    , ARRAY(CAST(label as DOUBLE)) as label
    , ARRAY(CAST(unseen_flag as BIGINT)) as unseen_flag
    -- , ARRAY(query_id) as query_id
    , IF(user_id is NULL,ARRAY(''),ARRAY(user_id)) as g_uid
    , IF(urb_seq_length is NULL,ARRAY(''),ARRAY(urb_seq_length)) as urb_seq_length
    , IF(preitem is NULL,ARRAY(''),SPLIT(preitem, UnicodeStr2UnicodeEncode("\\u001d"))) as preitem
    , IF(preshop is NULL,ARRAY(''),SPLIT(preshop, UnicodeStr2UnicodeEncode("\\u001d"))) as preshop
    , IF(prebrand is NULL,ARRAY(''),SPLIT(prebrand, UnicodeStr2UnicodeEncode("\\u001d"))) as prebrand
    , IF(precate is NULL,ARRAY(''),SPLIT(precate, UnicodeStr2UnicodeEncode("\\u001d"))) as precate
    , IF(prerootcate is NULL,ARRAY(''),SPLIT(prerootcate, UnicodeStr2UnicodeEncode("\\u001d"))) as prerootcate
    , IF(precity is NULL,ARRAY(''),SPLIT(precity, UnicodeStr2UnicodeEncode("\\u001d"))) as precity
    , IF(prequery is NULL,ARRAY(''),SPLIT(prequery, UnicodeStr2UnicodeEncode("\\u001d"))) as prequery
    , IF(prepage is NULL,ARRAY(''),SPLIT(prepage, UnicodeStr2UnicodeEncode("\\u001d"))) as prepage
    , IF(item_id is NULL,ARRAY(''),ARRAY(item_id)) as item_id
    , IF(clienttype_istop is NULL,ARRAY(''),ARRAY(clienttype_istop)) as clienttype_istop
    , IF(clienttype is NULL,ARRAY(''),ARRAY(clienttype)) as clienttype
    , IF(istop is NULL,ARRAY(''),ARRAY(istop)) as istop
    , IF(ad_cate_root is NULL,ARRAY(''),ARRAY(ad_cate_root)) as ad_cate_root
    , IF(ad_cate_leaf is NULL,ARRAY(''),ARRAY(ad_cate_leaf)) as ad_cate_leaf
    , IF(ad_brand_id is NULL,ARRAY(''),ARRAY(ad_brand_id)) as ad_brand_id
    , IF(ad_shopid is NULL,ARRAY(''),ARRAY(ad_shopid)) as ad_shopid
    , IF(ad_commodity_id is NULL,ARRAY(''),ARRAY(ad_commodity_id)) as ad_commodity_id
    , IF(ad_tradeid is NULL,ARRAY(''),ARRAY(ad_tradeid)) as ad_tradeid
    , IF(ad_cityid is NULL,ARRAY(''),ARRAY(ad_cityid)) as ad_cityid
    , IF(ad_priceid is NULL,ARRAY(''),ARRAY(ad_priceid)) as ad_priceid
    , IF(ad_b_or_c is NULL,ARRAY(''),ARRAY(ad_b_or_c)) as ad_b_or_c
    -- , ARRAY(COALESCE(GET_JSON_OBJECT(sample_id, '$.adgroup_id'),"0")) as adgroup_id
    , IF(adgroup_id is NULL,ARRAY(''),ARRAY(adgroup_id)) as adgroup_id
    , IF(upp_age is NULL,ARRAY(''),ARRAY(upp_age)) as upp_age
    , IF(upp_gender is NULL,ARRAY(''),ARRAY(upp_gender)) as upp_gender
    , IF(upp_occupation is NULL,ARRAY(''),ARRAY(upp_occupation)) as upp_occupation
    , IF(upp_province is NULL,ARRAY(''),ARRAY(upp_province)) as upp_province
    , IF(upp_city is NULL,ARRAY(''),ARRAY(upp_city)) as upp_city
    , IF(upp_u_star_level is NULL,ARRAY(''),ARRAY(upp_u_star_level)) as upp_u_star_level
    , IF(upp_u_perfer_cates is NULL,ARRAY(''),SPLIT(upp_u_perfer_cates, UnicodeStr2UnicodeEncode("\\u001d"))） as upp_u_perfer_cates
    , IF(upp_u_perfer_brandids is NULL,ARRAY(''),SPLIT(upp_u_perfer_brandids, UnicodeStr2UnicodeEncode("\\u001d"))） as upp_u_perfer_brandids
    , IF(upp_u_purchase_level is NULL,ARRAY(''),ARRAY(upp_u_purchase_level)) as upp_u_purchase_level
    , IF(upp_u_user_vip_level is NULL,ARRAY(''),ARRAY(upp_u_user_vip_level)) as upp_u_user_vip_level
    , IF(q_qid is NULL,ARRAY(''),ARRAY(q_qid)) as q_qid
    , IF(q_cate_prop is NULL,ARRAY(''),ARRAY(q_cate_prop)) as q_cate_prop
    , IF(q_primary_terms is NULL,ARRAY(''),SPLIT(q_primary_terms, UnicodeStr2UnicodeEncode("\\u001d"))） as q_primary_terms
    , IF(q_terms is NULL,ARRAY(''),SPLIT(q_terms, UnicodeStr2UnicodeEncode("\\u001d"))） as q_terms
    , IF(q_pcategory_id_list is NULL,ARRAY(''),SPLIT(q_pcategory_id_list, UnicodeStr2UnicodeEncode("\\u001d"))） as q_pcategory_id_list
    , IF(q_term_len is NULL,ARRAY(''),ARRAY(q_term_len)) as q_term_len
    , IF(q_unigram_len is NULL,ARRAY(''),ARRAY(q_unigram_len)) as q_unigram_len
    , IF(q_pcategory_len is NULL,ARRAY(''),ARRAY(q_pcategory_len)) as q_pcategory_len
    , IF(q_pcateid_level1 is NULL,ARRAY(''),ARRAY(q_pcateid_level1)) as q_pcateid_level1
FROM alimama_kgb_algo_dev.kgb_match_qy_opt03_mid_samples_kgb_neg_sample_res
where ds = '${bizdate}_weighted_root_leafcate' and tag = 'test'
    and CAST(unseen_flag as BIGINT) != -1 and CAST(unseen_flag as BIGINT) != -2 --unpv和neg sample都不进入test
distribute by rand() SORT BY RAND();