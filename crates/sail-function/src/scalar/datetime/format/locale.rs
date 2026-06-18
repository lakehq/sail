use super::pattern::LocaleSpec;

#[derive(Debug, Clone, Copy)]
pub(crate) struct LocaleData {
    pub(crate) months_short: [&'static str; 12],
    pub(crate) months_full: [&'static str; 12],
    pub(crate) weekdays_short: [&'static str; 7],
    pub(crate) weekdays_full: [&'static str; 7],
    pub(crate) quarters_short: [&'static str; 4],
    pub(crate) quarters_full: [&'static str; 4],
    pub(crate) eras_short: [&'static str; 2],
    pub(crate) eras_full: [&'static str; 2],
    pub(crate) eras_narrow: [&'static str; 2],
    pub(crate) am_pm: [&'static str; 2],
}

pub(crate) const EN_US: LocaleData = LocaleData {
    months_short: [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ],
    months_full: [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ],
    weekdays_short: ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
    weekdays_full: [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ],
    quarters_short: ["Q1", "Q2", "Q3", "Q4"],
    quarters_full: ["1st quarter", "2nd quarter", "3rd quarter", "4th quarter"],
    eras_short: ["BC", "AD"],
    eras_full: ["Before Christ", "Anno Domini"],
    eras_narrow: ["B", "A"],
    am_pm: ["AM", "PM"],
};

const FR_FR: LocaleData = LocaleData {
    months_short: [
        "janv.", "fevr.", "mars", "avr.", "mai", "juin", "juil.", "aout", "sept.", "oct.", "nov.", "dec.",
    ],
    months_full: [
        "janvier",
        "fevrier",
        "mars",
        "avril",
        "mai",
        "juin",
        "juillet",
        "aout",
        "septembre",
        "octobre",
        "novembre",
        "decembre",
    ],
    weekdays_short: ["lun.", "mar.", "mer.", "jeu.", "ven.", "sam.", "dim."],
    weekdays_full: ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"],
    quarters_short: ["T1", "T2", "T3", "T4"],
    quarters_full: ["1er trimestre", "2e trimestre", "3e trimestre", "4e trimestre"],
    eras_short: ["av. J.-C.", "ap. J.-C."],
    eras_full: ["avant Jesus-Christ", "apres Jesus-Christ"],
    eras_narrow: ["av. J.-C.", "ap. J.-C."],
    am_pm: ["AM", "PM"],
};

const DE_DE: LocaleData = LocaleData {
    months_short: [
        "Jan.", "Feb.", "Marz", "Apr.", "Mai", "Juni", "Juli", "Aug.", "Sept.", "Okt.", "Nov.", "Dez.",
    ],
    months_full: [
        "Januar",
        "Februar",
        "Marz",
        "April",
        "Mai",
        "Juni",
        "Juli",
        "August",
        "September",
        "Oktober",
        "November",
        "Dezember",
    ],
    weekdays_short: ["Mo.", "Di.", "Mi.", "Do.", "Fr.", "Sa.", "So."],
    weekdays_full: ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"],
    quarters_short: ["Q1", "Q2", "Q3", "Q4"],
    quarters_full: ["1. Quartal", "2. Quartal", "3. Quartal", "4. Quartal"],
    eras_short: ["v. Chr.", "n. Chr."],
    eras_full: ["vor Christus", "nach Christus"],
    eras_narrow: ["v. Chr.", "n. Chr."],
    am_pm: ["AM", "PM"],
};

const JA_JP: LocaleData = LocaleData {
    months_short: ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月"],
    months_full: ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月"],
    weekdays_short: ["月", "火", "水", "木", "金", "土", "日"],
    weekdays_full: ["月曜日", "火曜日", "水曜日", "木曜日", "金曜日", "土曜日", "日曜日"],
    quarters_short: ["Q1", "Q2", "Q3", "Q4"],
    quarters_full: ["第1四半期", "第2四半期", "第3四半期", "第4四半期"],
    eras_short: ["紀元前", "西暦"],
    eras_full: ["紀元前", "西暦"],
    eras_narrow: ["BC", "AD"],
    am_pm: ["午前", "午後"],
};

const ZH_CN: LocaleData = LocaleData {
    months_short: ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月"],
    months_full: ["一月", "二月", "三月", "四月", "五月", "六月", "七月", "八月", "九月", "十月", "十一月", "十二月"],
    weekdays_short: ["周一", "周二", "周三", "周四", "周五", "周六", "周日"],
    weekdays_full: ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"],
    quarters_short: ["1季度", "2季度", "3季度", "4季度"],
    quarters_full: ["第一季度", "第二季度", "第三季度", "第四季度"],
    eras_short: ["公元前", "公元"],
    eras_full: ["公元前", "公元"],
    eras_narrow: ["前", "公元"],
    am_pm: ["上午", "下午"],
};

impl LocaleSpec {
    pub(crate) fn data(&self) -> &'static LocaleData {
        match self {
            LocaleSpec::Default => &EN_US,
            LocaleSpec::Identifier(value) => locale_data(value),
        }
    }
}

fn locale_data(value: &str) -> &'static LocaleData {
    match normalize_locale(value).as_str() {
        "fr" | "fr_fr" => &FR_FR,
        "de" | "de_de" => &DE_DE,
        "ja" | "ja_jp" => &JA_JP,
        "zh" | "zh_cn" => &ZH_CN,
        _ => &EN_US,
    }
}

fn normalize_locale(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('-', "_")
}