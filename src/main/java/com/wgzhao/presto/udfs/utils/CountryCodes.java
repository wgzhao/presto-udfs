package com.wgzhao.presto.udfs.utils;

import java.util.HashMap;
import java.util.Map;

public class CountryCodes
{

    private static final Map<String, String> countryCodes = new HashMap<>();
    private static final Map<String, Integer> codeMap = new HashMap<>();

    public static boolean isValidCode(String code)
    {
        return codeMap.get(code) != null;
    }

    public static String getCode(String country)
    {
        if (country == null || countryCodes.get(country) == null) {
            return null;
        }
        return countryCodes.get(country);
    }

    public static String getCode(String country, String code)
    {
        if (country == null || code == null) {
            return null;
        }
        String record = countryCodes.get(country);
        Integer pos = codeMap.get(code);
        if (record == null || pos == null) {
            return null;
        }
        return record.split("\\|")[pos];
    }

    static {
        countryCodes.put("阿富汗", "Afghanistan|AF|AFG|4");
        countryCodes.put("奥兰", "Åland Islands|AX|ALA|248");
        countryCodes.put("阿尔巴尼亚", "Albania|AL|ALB|8");
        countryCodes.put("阿尔及利亚", "Algeria|DZ|DZA|12");
        countryCodes.put("美属萨摩亚", "American Samoa|AS|ASM|16");
        countryCodes.put("安道尔", "Andorra|AD|AND|20");
        countryCodes.put("安哥拉", "Angola|AO|AGO|24");
        countryCodes.put("安圭拉", "Anguilla|AI|AIA|660");
        countryCodes.put("南极洲", "Antarctica|AQ|ATA|10");
        countryCodes.put("安提瓜和巴布达", "Antigua and Barbuda|AG|ATG|28");
        countryCodes.put("阿根廷", "Argentina|AR|ARG|32");
        countryCodes.put("亚美尼亚", "Armenia|AM|ARM|51");
        countryCodes.put("阿鲁巴", "Aruba|AW|ABW|533");
        countryCodes.put("澳大利亚", "Australia|AU|AUS|36");
        countryCodes.put("奥地利", "Austria|AT|AUT|40");
        countryCodes.put("阿塞拜疆", "Azerbaijan|AZ|AZE|31");
        countryCodes.put("巴哈马", "Bahamas|BS|BHS|44");
        countryCodes.put("巴林", "Bahrain|BH|BHR|48");
        countryCodes.put("孟加拉国", "Bangladesh|BD|BGD|50");
        countryCodes.put("巴巴多斯", "Barbados|BB|BRB|52");
        countryCodes.put("白俄罗斯", "Belarus|BY|BLR|112");
        countryCodes.put("比利时", "Belgium|BE|BEL|56");
        countryCodes.put("伯利兹", "Belize|BZ|BLZ|84");
        countryCodes.put("贝宁", "Benin|BJ|BEN|204");
        countryCodes.put("百慕大", "Bermuda|BM|BMU|60");
        countryCodes.put("不丹", "Bhutan|BT|BTN|64");
        countryCodes.put("玻利维亚", "Bolivia (Plurinational State of)|BO|BOL|68");
        countryCodes.put("荷兰加勒比区", "Bonaire, Sint Eustatius and Saba|BQ|BES|535");
        countryCodes.put("波黑", "Bosnia and Herzegovina|BA|BIH|70");
        countryCodes.put("博茨瓦纳", "Botswana|BW|BWA|72");
        countryCodes.put("布韦岛", "Bouvet Island|BV|BVT|74");
        countryCodes.put("巴西", "Brazil|BR|BRA|76");
        countryCodes.put("英属印度洋领地", "British Indian Ocean Territory|IO|IOT|86");
        countryCodes.put("文莱", "Brunei Darussalam|BN|BRN|96");
        countryCodes.put("保加利亚", "Bulgaria|BG|BGR|100");
        countryCodes.put("布基纳法索", "Burkina Faso|BF|BFA|854");
        countryCodes.put("布隆迪", "Burundi|BI|BDI|108");
        countryCodes.put("佛得角", "Cabo Verde|CV|CPV|132");
        countryCodes.put("柬埔寨", "Cambodia|KH|KHM|116");
        countryCodes.put("喀麦隆", "Cameroon|CM|CMR|120");
        countryCodes.put("加拿大", "Canada|CA|CAN|124");
        countryCodes.put("开曼群岛", "Cayman Islands|KY|CYM|136");
        countryCodes.put("中非", "Central African Republic|CF|CAF|140");
        countryCodes.put("乍得", "Chad|TD|TCD|148");
        countryCodes.put("智利", "Chile|CL|CHL|152");
        countryCodes.put("中国", "China|CN|CHN|156");
        countryCodes.put("圣诞岛", "Christmas Island|CX|CXR|162");
        countryCodes.put("科科斯（基林）群岛", "Cocos (Keeling) Islands|CC|CCK|166");
        countryCodes.put("哥伦比亚", "Colombia|CO|COL|170");
        countryCodes.put("科摩罗", "Comoros|KM|COM|174");
        countryCodes.put("刚果（布）", "Congo|CG|COG|178");
        countryCodes.put("刚果（金）", "Congo (Democratic Republic of the)|CD|COD|180");
        countryCodes.put("库克群岛", "Cook Islands|CK|COK|184");
        countryCodes.put("哥斯达黎加", "Costa Rica|CR|CRI|188");
        countryCodes.put("科特迪瓦", "Côte d'Ivoire|CI|CIV|384");
        countryCodes.put("克罗地亚", "Croatia|HR|HRV|191");
        countryCodes.put("古巴", "Cuba|CU|CUB|192");
        countryCodes.put("库拉索", "Curaçao|CW|CUW|531");
        countryCodes.put("塞浦路斯", "Cyprus|CY|CYP|196");
        countryCodes.put("捷克", "Czechia|CZ|CZE|203");
        countryCodes.put("丹麦", "Denmark|DK|DNK|208");
        countryCodes.put("吉布提", "Djibouti|DJ|DJI|262");
        countryCodes.put("多米尼克", "Dominica|DM|DMA|212");
        countryCodes.put("多米尼加", "Dominican Republic|DO|DOM|214");
        countryCodes.put("厄瓜多尔", "Ecuador|EC|ECU|218");
        countryCodes.put("埃及", "Egypt|EG|EGY|818");
        countryCodes.put("萨尔瓦多", "El Salvador|SV|SLV|222");
        countryCodes.put("赤道几内亚", "Equatorial Guinea|GQ|GNQ|226");
        countryCodes.put("厄立特里亚", "Eritrea|ER|ERI|232");
        countryCodes.put("爱沙尼亚", "Estonia|EE|EST|233");
        countryCodes.put("斯威士兰", "Eswatini|SZ|SWZ|748");
        countryCodes.put("埃塞俄比亚", "Ethiopia|ET|ETH|231");
        countryCodes.put("福克兰群岛", "Falkland Islands (Malvinas)|FK|FLK|238");
        countryCodes.put("法罗群岛", "Faroe Islands|FO|FRO|234");
        countryCodes.put("斐济", "Fiji|FJ|FJI|242");
        countryCodes.put("芬兰", "Finland|FI|FIN|246");
        countryCodes.put("法国", "France|FR|FRA|250");
        countryCodes.put("法属圭亚那", "French Guiana|GF|GUF|254");
        countryCodes.put("法属波利尼西亚", "French Polynesia|PF|PYF|258");
        countryCodes.put("法属南方和南极洲领地", "French Southern Territories|TF|ATF|260");
        countryCodes.put("加蓬", "Gabon|GA|GAB|266");
        countryCodes.put("冈比亚", "Gambia|GM|GMB|270");
        countryCodes.put("格鲁吉亚", "Georgia|GE|GEO|268");
        countryCodes.put("德国", "Germany|DE|DEU|276");
        countryCodes.put("加纳", "Ghana|GH|GHA|288");
        countryCodes.put("直布罗陀", "Gibraltar|GI|GIB|292");
        countryCodes.put("希腊", "Greece|GR|GRC|300");
        countryCodes.put("格陵兰", "Greenland|GL|GRL|304");
        countryCodes.put("格林纳达", "Grenada|GD|GRD|308");
        countryCodes.put("瓜德罗普", "Guadeloupe|GP|GLP|312");
        countryCodes.put("关岛", "Guam|GU|GUM|316");
        countryCodes.put("危地马拉", "Guatemala|GT|GTM|320");
        countryCodes.put("根西", "Guernsey|GG|GGY|831");
        countryCodes.put("几内亚", "Guinea|GN|GIN|324");
        countryCodes.put("几内亚比绍", "Guinea-Bissau|GW|GNB|624");
        countryCodes.put("圭亚那", "Guyana|GY|GUY|328");
        countryCodes.put("海地", "Haiti|HT|HTI|332");
        countryCodes.put("赫德岛和麦克唐纳群岛", "Heard Island and McDonald Islands|HM|HMD|334");
        countryCodes.put("梵蒂冈", "Holy See|VA|VAT|336");
        countryCodes.put("洪都拉斯", "Honduras|HN|HND|340");
        countryCodes.put("香港", "Hong Kong|HK|HKG|344");
        countryCodes.put("匈牙利", "Hungary|HU|HUN|348");
        countryCodes.put("冰岛", "Iceland|IS|ISL|352");
        countryCodes.put("印度", "India|IN|IND|356");
        countryCodes.put("印尼", "Indonesia|ID|IDN|360");
        countryCodes.put("伊朗", "Iran (Islamic Republic of)|IR|IRN|364");
        countryCodes.put("伊拉克", "Iraq|IQ|IRQ|368");
        countryCodes.put("爱尔兰", "Ireland|IE|IRL|372");
        countryCodes.put("马恩岛", "Isle of Man|IM|IMN|833");
        countryCodes.put("以色列", "Israel|IL|ISR|376");
        countryCodes.put("意大利", "Italy|IT|ITA|380");
        countryCodes.put("牙买加", "Jamaica|JM|JAM|388");
        countryCodes.put("日本", "Japan|JP|JPN|392");
        countryCodes.put("泽西", "Jersey|JE|JEY|832");
        countryCodes.put("约旦", "Jordan|JO|JOR|400");
        countryCodes.put("哈萨克斯坦", "Kazakhstan|KZ|KAZ|398");
        countryCodes.put("肯尼亚", "Kenya|KE|KEN|404");
        countryCodes.put("基里巴斯", "Kiribati|KI|KIR|296");
        countryCodes.put("朝鲜", "Korea (Democratic People's Republic of)|KP|PRK|408");
        countryCodes.put("韩国", "Korea (Republic of)|KR|KOR|410");
        countryCodes.put("科威特", "Kuwait|KW|KWT|414");
        countryCodes.put("吉尔吉斯斯坦", "Kyrgyzstan|KG|KGZ|417");
        countryCodes.put("老挝", "Lao People's Democratic Republic|LA|LAO|418");
        countryCodes.put("拉脱维亚", "Latvia|LV|LVA|428");
        countryCodes.put("黎巴嫩", "Lebanon|LB|LBN|422");
        countryCodes.put("莱索托", "Lesotho|LS|LSO|426");
        countryCodes.put("利比里亚", "Liberia|LR|LBR|430");
        countryCodes.put("利比亚", "Libya|LY|LBY|434");
        countryCodes.put("列支敦士登", "Liechtenstein|LI|LIE|438");
        countryCodes.put("立陶宛", "Lithuania|LT|LTU|440");
        countryCodes.put("卢森堡", "Luxembourg|LU|LUX|442");
        countryCodes.put("澳门", "Macao|MO|MAC|446");
        countryCodes.put("马达加斯加", "Madagascar|MG|MDG|450");
        countryCodes.put("马拉维", "Malawi|MW|MWI|454");
        countryCodes.put("马来西亚", "Malaysia|MY|MYS|458");
        countryCodes.put("马尔代夫", "Maldives|MV|MDV|462");
        countryCodes.put("马里", "Mali|ML|MLI|466");
        countryCodes.put("马耳他", "Malta|MT|MLT|470");
        countryCodes.put("马绍尔群岛", "Marshall Islands|MH|MHL|584");
        countryCodes.put("马提尼克", "Martinique|MQ|MTQ|474");
        countryCodes.put("毛里塔尼亚", "Mauritania|MR|MRT|478");
        countryCodes.put("毛里求斯", "Mauritius|MU|MUS|480");
        countryCodes.put("马约特", "Mayotte|YT|MYT|175");
        countryCodes.put("墨西哥", "Mexico|MX|MEX|484");
        countryCodes.put("密克罗尼西亚联邦", "Micronesia (Federated States of)|FM|FSM|583");
        countryCodes.put("摩尔多瓦", "Moldova (Republic of)|MD|MDA|498");
        countryCodes.put("摩纳哥", "Monaco|MC|MCO|492");
        countryCodes.put("蒙古国", "Mongolia|MN|MNG|496");
        countryCodes.put("黑山", "Montenegro|ME|MNE|499");
        countryCodes.put("蒙特塞拉特", "Montserrat|MS|MSR|500");
        countryCodes.put("摩洛哥", "Morocco|MA|MAR|504");
        countryCodes.put("莫桑比克", "Mozambique|MZ|MOZ|508");
        countryCodes.put("缅甸", "Myanmar|MM|MMR|104");
        countryCodes.put("纳米比亚", "Namibia|nan|NAM|516");
        countryCodes.put("瑙鲁", "Nauru|NR|NRU|520");
        countryCodes.put("尼泊尔", "Nepal|NP|NPL|524");
        countryCodes.put("荷兰", "Netherlands|NL|NLD|528");
        countryCodes.put("新喀里多尼亚", "New Caledonia|NC|NCL|540");
        countryCodes.put("新西兰", "New Zealand|NZ|NZL|554");
        countryCodes.put("尼加拉瓜", "Nicaragua|NI|NIC|558");
        countryCodes.put("尼日尔", "Niger|NE|NER|562");
        countryCodes.put("尼日利亚", "Nigeria|NG|NGA|566");
        countryCodes.put("纽埃", "Niue|NU|NIU|570");
        countryCodes.put("诺福克岛", "Norfolk Island|NF|NFK|574");
        countryCodes.put("北马其顿", "North Macedonia|MK|MKD|807");
        countryCodes.put("北马里亚纳群岛", "Northern Mariana Islands|MP|MNP|580");
        countryCodes.put("挪威", "Norway|NO|NOR|578");
        countryCodes.put("阿曼", "Oman|OM|OMN|512");
        countryCodes.put("巴基斯坦", "Pakistan|PK|PAK|586");
        countryCodes.put("帕劳", "Palau|PW|PLW|585");
        countryCodes.put("巴勒斯坦", "Palestine, State of|PS|PSE|275");
        countryCodes.put("巴拿马", "Panama|PA|PAN|591");
        countryCodes.put("巴布亚新几内亚", "Papua New Guinea|PG|PNG|598");
        countryCodes.put("巴拉圭", "Paraguay|PY|PRY|600");
        countryCodes.put("秘鲁", "Peru|PE|PER|604");
        countryCodes.put("菲律宾", "Philippines|PH|PHL|608");
        countryCodes.put("皮特凯恩群岛", "Pitcairn|PN|PCN|612");
        countryCodes.put("波兰", "Poland|PL|POL|616");
        countryCodes.put("葡萄牙", "Portugal|PT|PRT|620");
        countryCodes.put("波多黎各", "Puerto Rico|PR|PRI|630");
        countryCodes.put("卡塔尔", "Qatar|QA|QAT|634");
        countryCodes.put("留尼汪", "Réunion|RE|REU|638");
        countryCodes.put("罗马尼亚", "Romania|RO|ROU|642");
        countryCodes.put("俄罗斯", "Russian Federation|RU|RUS|643");
        countryCodes.put("卢旺达", "Rwanda|RW|RWA|646");
        countryCodes.put("圣巴泰勒米", "Saint Barthélemy|BL|BLM|652");
        countryCodes.put("圣赫勒拿、阿森松和特里斯坦-达库尼亚", "Saint Helena, Ascension and Tristan da Cunha|SH|SHN|654");
        countryCodes.put("圣基茨和尼维斯", "Saint Kitts and Nevis|KN|KNA|659");
        countryCodes.put("圣卢西亚", "Saint Lucia|LC|LCA|662");
        countryCodes.put("法属圣马丁", "Saint Martin (French part)|MF|MAF|663");
        countryCodes.put("圣皮埃尔和密克隆", "Saint Pierre and Miquelon|PM|SPM|666");
        countryCodes.put("圣文森特和格林纳丁斯", "Saint Vincent and the Grenadines|VC|VCT|670");
        countryCodes.put("萨摩亚", "Samoa|WS|WSM|882");
        countryCodes.put("圣马力诺", "San Marino|SM|SMR|674");
        countryCodes.put("圣多美和普林西比", "Sao Tome and Principe|ST|STP|678");
        countryCodes.put("沙特阿拉伯", "Saudi Arabia|SA|SAU|682");
        countryCodes.put("塞内加尔", "Senegal|SN|SEN|686");
        countryCodes.put("塞尔维亚", "Serbia|RS|SRB|688");
        countryCodes.put("塞舌尔", "Seychelles|SC|SYC|690");
        countryCodes.put("塞拉利昂", "Sierra Leone|SL|SLE|694");
        countryCodes.put("新加坡", "Singapore|SG|SGP|702");
        countryCodes.put("荷属圣马丁", "Sint Maarten (Dutch part)|SX|SXM|534");
        countryCodes.put("斯洛伐克", "Slovakia|SK|SVK|703");
        countryCodes.put("斯洛文尼亚", "Slovenia|SI|SVN|705");
        countryCodes.put("所罗门群岛", "Solomon Islands|SB|SLB|90");
        countryCodes.put("索马里", "Somalia|SO|SOM|706");
        countryCodes.put("南非", "South Africa|ZA|ZAF|710");
        countryCodes.put("南乔治亚和南桑威奇群岛", "South Georgia and the South Sandwich Islands|GS|SGS|239");
        countryCodes.put("南苏丹", "South Sudan|SS|SSD|728");
        countryCodes.put("西班牙", "Spain|ES|ESP|724");
        countryCodes.put("斯里兰卡", "Sri Lanka|LK|LKA|144");
        countryCodes.put("苏丹", "Sudan|SD|SDN|729");
        countryCodes.put("苏里南", "Suriname|SR|SUR|740");
        countryCodes.put("斯瓦尔巴和扬马延", "Svalbard and Jan Mayen|SJ|SJM|744");
        countryCodes.put("瑞典", "Sweden|SE|SWE|752");
        countryCodes.put("瑞士", "Switzerland|CH|CHE|756");
        countryCodes.put("叙利亚", "Syrian Arab Republic|SY|SYR|760");
        countryCodes.put("中国台湾省", "Taiwan, Provincque of China|TW|TWN|158");
        countryCodes.put("台湾", "Taiwan, Province of China|TW|TWN|158");
        countryCodes.put("台湾省", "Taiwan, Province of China|TW|TWN|158");
        countryCodes.put("塔吉克斯坦", "Tajikistan|TJ|TJK|762");
        countryCodes.put("坦桑尼亚", "Tanzania, United Republic of|TZ|TZA|834");
        countryCodes.put("泰国", "Thailand|TH|THA|764");
        countryCodes.put("东帝汶", "Timor-Leste|TL|TLS|626");
        countryCodes.put("多哥", "Togo|TG|TGO|768");
        countryCodes.put("托克劳", "Tokelau|TK|TKL|772");
        countryCodes.put("汤加", "Tonga|TO|TON|776");
        countryCodes.put("特立尼达和多巴哥", "Trinidad and Tobago|TT|TTO|780");
        countryCodes.put("突尼斯", "Tunisia|TN|TUN|788");
        countryCodes.put("土耳其", "Turkey|TR|TUR|792");
        countryCodes.put("土库曼斯坦", "Turkmenistan|TM|TKM|795");
        countryCodes.put("特克斯和凯科斯群岛", "Turks and Caicos Islands|TC|TCA|796");
        countryCodes.put("图瓦卢", "Tuvalu|TV|TUV|798");
        countryCodes.put("乌干达", "Uganda|UG|UGA|800");
        countryCodes.put("乌克兰", "Ukraine|UA|UKR|804");
        countryCodes.put("阿联酋", "United Arab Emirates|AE|ARE|784");
        countryCodes.put("英国", "United Kingdom of Great Britain and Northern Ireland|GB|GBR|826");
        countryCodes.put("美国", "United States of America|US|USA|840");
        countryCodes.put("美国本土外小岛屿", "United States Minor Outlying Islands|UM|UMI|581");
        countryCodes.put("乌拉圭", "Uruguay|UY|URY|858");
        countryCodes.put("乌兹别克斯坦", "Uzbekistan|UZ|UZB|860");
        countryCodes.put("瓦努阿图", "Vanuatu|VU|VUT|548");
        countryCodes.put("委内瑞拉", "Venezuela (Bolivarian Republic of)|VE|VEN|862");
        countryCodes.put("越南", "Viet Nam|VN|VNM|704");
        countryCodes.put("英属维尔京群岛", "Virgin Islands (British)|VG|VGB|92");
        countryCodes.put("美属维尔京群岛", "Virgin Islands (U.S.)|VI|VIR|850");
        countryCodes.put("瓦利斯和富图纳", "Wallis and Futuna|WF|WLF|876");
        countryCodes.put("阿拉伯撒哈拉民主共和国", "Western Sahara|EH|ESH|732");
        countryCodes.put("也门", "Yemen|YE|YEM|887");
        countryCodes.put("赞比亚", "Zambia|ZM|ZMB|894");
        countryCodes.put("津巴布韦", "Zimbabwe|ZW|ZWE|716");
    }

    static {
        codeMap.put("en", 0);
        codeMap.put("m2", 1);
        codeMap.put("m3", 2);
        codeMap.put("digit", 3);
    }
}