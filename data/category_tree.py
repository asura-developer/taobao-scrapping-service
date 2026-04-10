"""
Canonical Taobao/Tmall/1688 category tree.

Two-level hierarchy:
  GroupCategory  (parent)  — e.g. Electronics, Women's Clothing
  SubCategory    (child)   — e.g. Electronics → Mobile Phones, Laptops

All objects are frozen dataclasses (immutable).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# ── Data structures ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SubCategory:
    sub_id: str            # stable slug, e.g. "phones"
    name_zh: str           # Chinese name
    name_en: str           # English name
    # Platform-native category IDs (None = not directly addressable)
    taobao_id: Optional[str] = None
    tmall_id: Optional[str] = None
    id_1688: Optional[str] = None


@dataclass(frozen=True)
class GroupCategory:
    group_id: str                          # stable slug, e.g. "electronics"
    name_zh: str                           # Chinese name
    name_en: str                           # English name
    subs: tuple[SubCategory, ...]          # child sub-categories


# ── Category tree definition ───────────────────────────────────────────────────

CATEGORY_TREE: dict[str, GroupCategory] = {
    "womens_clothing": GroupCategory(
        group_id="womens_clothing",
        name_zh="女装",
        name_en="Women's Clothing",
        subs=(
            SubCategory("womens_dresses",       "连衣裙",    "Dresses",           taobao_id="50014866", tmall_id="50014866"),
            SubCategory("womens_tshirts",        "T恤",      "T-Shirts",          taobao_id="50025969"),
            SubCategory("womens_blouses",        "衬衫",     "Blouses"),
            SubCategory("womens_coats",          "外套",     "Coats & Jackets"),
            SubCategory("womens_sweaters",       "毛衣/针织", "Sweaters/Knitwear"),
            SubCategory("womens_pants",          "裤子",     "Pants"),
            SubCategory("womens_skirts",         "半身裙",   "Skirts"),
            SubCategory("womens_jeans",          "牛仔裤",   "Jeans"),
            SubCategory("womens_down_jackets",   "羽绒服",   "Down Jackets"),
            SubCategory("womens_plus_size",      "大码女装",  "Plus Size"),
            SubCategory("womens_lingerie",       "内衣",     "Lingerie"),
            SubCategory("womens_sleepwear",      "睡衣",     "Sleepwear"),
        ),
    ),
    "mens_clothing": GroupCategory(
        group_id="mens_clothing",
        name_zh="男装",
        name_en="Men's Clothing",
        subs=(
            SubCategory("mens_tshirts",          "T恤",      "T-Shirts"),
            SubCategory("mens_shirts",           "衬衫",     "Shirts"),
            SubCategory("mens_jackets",          "外套",     "Jackets"),
            SubCategory("mens_pants",            "裤子",     "Pants"),
            SubCategory("mens_jeans",            "牛仔裤",   "Jeans"),
            SubCategory("mens_down_jackets",     "羽绒服",   "Down Jackets"),
            SubCategory("mens_suits",            "西装",     "Suits"),
            SubCategory("mens_sweaters",         "毛衣",     "Sweaters"),
            SubCategory("mens_sportswear",       "运动服",   "Sportswear"),
            SubCategory("mens_underwear",        "内衣",     "Underwear"),
        ),
    ),
    "shoes": GroupCategory(
        group_id="shoes",
        name_zh="鞋靴",
        name_en="Shoes & Boots",
        subs=(
            SubCategory("shoes_womens",          "女鞋",     "Women's Shoes"),
            SubCategory("shoes_mens",            "男鞋",     "Men's Shoes"),
            SubCategory("shoes_sneakers",        "运动鞋",   "Sneakers"),
            SubCategory("shoes_boots",           "靴子",     "Boots"),
            SubCategory("shoes_sandals",         "凉鞋/拖鞋","Sandals & Slippers"),
            SubCategory("shoes_canvas",          "帆布鞋",   "Canvas Shoes"),
            SubCategory("shoes_leather",         "皮鞋",     "Leather Shoes"),
            SubCategory("shoes_kids",            "童鞋",     "Kids' Shoes"),
        ),
    ),
    "bags_luggage": GroupCategory(
        group_id="bags_luggage",
        name_zh="箱包",
        name_en="Bags & Luggage",
        subs=(
            SubCategory("bags_womens",           "女包",     "Women's Bags"),
            SubCategory("bags_mens",             "男包",     "Men's Bags"),
            SubCategory("bags_backpacks",        "双肩包",   "Backpacks"),
            SubCategory("bags_luggage",          "行李箱",   "Luggage"),
            SubCategory("bags_wallets",          "钱包",     "Wallets"),
            SubCategory("bags_handbags",         "手提包",   "Handbags"),
            SubCategory("bags_shoulder",         "单肩包",   "Shoulder Bags"),
            SubCategory("bags_waist",            "腰包",     "Waist Bags"),
        ),
    ),
    "beauty_skincare": GroupCategory(
        group_id="beauty_skincare",
        name_zh="美妆护肤",
        name_en="Beauty & Skincare",
        subs=(
            SubCategory("beauty_facial",         "面部护肤",  "Facial Skincare",   taobao_id="50025969"),
            SubCategory("beauty_makeup",         "彩妆",     "Makeup"),
            SubCategory("beauty_perfume",        "香水",     "Perfume"),
            SubCategory("beauty_tools",          "美容工具",  "Beauty Tools"),
            SubCategory("beauty_face_masks",     "面膜",     "Face Masks"),
            SubCategory("beauty_sunscreen",      "防晒",     "Sunscreen"),
            SubCategory("beauty_remover",        "卸妆",     "Makeup Remover"),
            SubCategory("beauty_mens",           "男士护肤",  "Men's Skincare"),
        ),
    ),
    "personal_care": GroupCategory(
        group_id="personal_care",
        name_zh="个人护理",
        name_en="Personal Care",
        subs=(
            SubCategory("care_hair",             "洗发护发",  "Hair Care"),
            SubCategory("care_body",             "身体护理",  "Body Care"),
            SubCategory("care_oral",             "口腔护理",  "Oral Care"),
            SubCategory("care_hair_tools",       "美发工具",  "Hair Tools"),
            SubCategory("care_shaving",          "剃须",     "Shaving"),
            SubCategory("care_hygiene",          "卫生用品",  "Hygiene Products"),
        ),
    ),
    "phones_digital": GroupCategory(
        group_id="phones_digital",
        name_zh="手机数码",
        name_en="Phones & Digital",
        subs=(
            SubCategory("digital_phones",        "手机",         "Mobile Phones",       taobao_id="50014811"),
            SubCategory("digital_cases",         "手机壳/膜",     "Cases & Screen Protectors"),
            SubCategory("digital_chargers",      "充电器/数据线", "Chargers & Cables"),
            SubCategory("digital_power_banks",   "移动电源",      "Power Banks"),
            SubCategory("digital_earphones",     "蓝牙耳机",      "Bluetooth Earphones"),
            SubCategory("digital_smartwatch",    "智能手表",      "Smart Watches"),
            SubCategory("digital_tablets",       "平板电脑",      "Tablets"),
            SubCategory("digital_accessories",   "手机配件",      "Phone Accessories"),
        ),
    ),
    "computers_office": GroupCategory(
        group_id="computers_office",
        name_zh="电脑办公",
        name_en="Computers & Office",
        subs=(
            SubCategory("pc_laptops",            "笔记本电脑",  "Laptops"),
            SubCategory("pc_desktops",           "台式机",     "Desktops"),
            SubCategory("pc_monitors",           "显示器",     "Monitors"),
            SubCategory("pc_keyboards_mice",     "键盘鼠标",   "Keyboards & Mice"),
            SubCategory("pc_printers",           "打印机",     "Printers"),
            SubCategory("pc_routers",            "路由器",     "Routers"),
            SubCategory("pc_accessories",        "电脑配件",   "PC Accessories"),
            SubCategory("pc_office_supplies",    "办公用品",   "Office Supplies"),
            SubCategory("pc_storage",            "U盘/存储",   "USB Drives & Storage"),
        ),
    ),
    "home_appliances": GroupCategory(
        group_id="home_appliances",
        name_zh="家用电器",
        name_en="Home Appliances",
        subs=(
            SubCategory("appliance_refrigerator","冰箱",     "Refrigerators"),
            SubCategory("appliance_washer",      "洗衣机",   "Washing Machines"),
            SubCategory("appliance_ac",          "空调",     "Air Conditioners"),
            SubCategory("appliance_tv",          "电视",     "TVs"),
            SubCategory("appliance_small",       "小家电",   "Small Appliances"),
            SubCategory("appliance_rice_cooker", "电饭煲",   "Rice Cookers"),
            SubCategory("appliance_vacuum",      "吸尘器",   "Vacuum Cleaners"),
            SubCategory("appliance_air_purifier","空气净化器","Air Purifiers"),
            SubCategory("appliance_microwave",   "微波炉/烤箱","Microwave & Oven"),
        ),
    ),
    "home_living": GroupCategory(
        group_id="home_living",
        name_zh="家居家装",
        name_en="Home & Living",
        subs=(
            SubCategory("home_textiles",         "家纺",     "Home Textiles",     taobao_id="50010404"),
            SubCategory("home_lighting",         "灯具",     "Lighting"),
            SubCategory("home_bathroom",         "卫浴",     "Bathroom Fixtures"),
            SubCategory("home_kitchenware",      "厨房用品", "Kitchenware"),
            SubCategory("home_storage",          "收纳整理", "Storage & Organization"),
            SubCategory("home_furniture",        "家具",     "Furniture"),
            SubCategory("home_decor",            "装饰摆件", "Decorations"),
            SubCategory("home_curtains",         "窗帘",     "Curtains"),
            SubCategory("home_rugs",             "地毯",     "Rugs"),
            SubCategory("home_wallpaper",        "壁纸",     "Wallpaper"),
        ),
    ),
    "mother_baby": GroupCategory(
        group_id="mother_baby",
        name_zh="母婴用品",
        name_en="Mother & Baby",
        subs=(
            SubCategory("baby_formula",          "奶粉",     "Baby Formula",      taobao_id="50014866"),
            SubCategory("baby_diapers",          "纸尿裤",   "Diapers"),
            SubCategory("baby_clothing",         "婴儿服饰", "Baby Clothing"),
            SubCategory("baby_strollers",        "婴儿推车", "Strollers"),
            SubCategory("baby_toys",             "儿童玩具", "Children's Toys"),
            SubCategory("baby_maternity",        "孕妇装",   "Maternity Wear"),
            SubCategory("baby_feeding",          "喂养用品", "Feeding Supplies"),
            SubCategory("baby_kids_clothing",    "童装",     "Kids' Clothing"),
            SubCategory("baby_car_seats",        "安全座椅", "Car Seats"),
        ),
    ),
    "food_fresh": GroupCategory(
        group_id="food_fresh",
        name_zh="食品生鲜",
        name_en="Food & Fresh Produce",
        subs=(
            SubCategory("food_snacks",           "零食/坚果", "Snacks & Nuts"),
            SubCategory("food_tea",              "茶叶",     "Tea"),
            SubCategory("food_alcohol",          "酒类",     "Alcohol"),
            SubCategory("food_grains_oils",      "粮油调味", "Grains, Oils & Seasoning"),
            SubCategory("food_fresh_fruits",     "生鲜水果", "Fresh Fruits"),
            SubCategory("food_meat_eggs",        "肉禽蛋",   "Meat, Poultry & Eggs"),
            SubCategory("food_seafood",          "海鲜水产", "Seafood"),
            SubCategory("food_dairy",            "乳制品",   "Dairy"),
            SubCategory("food_supplements",      "保健品",   "Health Supplements"),
        ),
    ),
    "sports_outdoors": GroupCategory(
        group_id="sports_outdoors",
        name_zh="运动户外",
        name_en="Sports & Outdoors",
        subs=(
            SubCategory("sports_athletic_wear",  "运动鞋服", "Athletic Wear",     taobao_id="50016348"),
            SubCategory("sports_fitness",        "健身器材", "Fitness Equipment"),
            SubCategory("sports_outdoor_gear",   "户外装备", "Outdoor Gear"),
            SubCategory("sports_cycling",        "骑行装备", "Cycling Gear"),
            SubCategory("sports_swimming",       "游泳用品", "Swimming"),
            SubCategory("sports_ball",           "球类运动", "Ball Sports"),
            SubCategory("sports_yoga",           "瑜伽用品", "Yoga"),
            SubCategory("sports_camping",        "露营装备", "Camping"),
            SubCategory("sports_fishing",        "钓鱼用品", "Fishing"),
        ),
    ),
    "jewelry_accessories": GroupCategory(
        group_id="jewelry_accessories",
        name_zh="珠宝饰品",
        name_en="Jewelry & Accessories",
        subs=(
            SubCategory("jewelry_necklaces",     "项链",     "Necklaces"),
            SubCategory("jewelry_rings",         "戒指",     "Rings"),
            SubCategory("jewelry_earrings",      "耳饰",     "Earrings"),
            SubCategory("jewelry_bracelets",     "手链/手镯","Bracelets & Bangles"),
            SubCategory("jewelry_watches",       "手表",     "Watches"),
            SubCategory("jewelry_sunglasses",    "太阳镜",   "Sunglasses"),
            SubCategory("jewelry_hair_acc",      "发饰",     "Hair Accessories"),
            SubCategory("jewelry_brooches",      "胸针",     "Brooches"),
        ),
    ),
    "auto_accessories": GroupCategory(
        group_id="auto_accessories",
        name_zh="汽车用品",
        name_en="Auto Accessories",
        subs=(
            SubCategory("auto_electronics",      "车载电器",   "Car Electronics"),
            SubCategory("auto_seat_mats",        "座垫/脚垫", "Seat & Floor Mats"),
            SubCategory("auto_freshener",        "车载香水",   "Car Fresheners"),
            SubCategory("auto_dash_cam",         "行车记录仪", "Dash Cams"),
            SubCategory("auto_tires",            "轮胎",      "Tires"),
            SubCategory("auto_motor_oil",        "机油",      "Motor Oil"),
            SubCategory("auto_car_cover",        "车衣",      "Car Covers"),
            SubCategory("auto_safety",           "安全用品",  "Safety Accessories"),
        ),
    ),
    "toys_instruments": GroupCategory(
        group_id="toys_instruments",
        name_zh="玩具乐器",
        name_en="Toys & Musical Instruments",
        subs=(
            SubCategory("toys_rc",               "遥控玩具",  "RC Toys"),
            SubCategory("toys_building",         "拼装模型",  "Building Models"),
            SubCategory("toys_plush",            "毛绒玩具",  "Plush Toys"),
            SubCategory("toys_educational",      "益智玩具",  "Educational Toys"),
            SubCategory("instruments_guitar",    "吉他",     "Guitars"),
            SubCategory("instruments_piano",     "钢琴/电子琴","Piano & Keyboard"),
            SubCategory("instruments_acc",       "乐器配件",  "Instrument Accessories"),
        ),
    ),
    "books_stationery": GroupCategory(
        group_id="books_stationery",
        name_zh="图书文具",
        name_en="Books & Stationery",
        subs=(
            SubCategory("books",                 "图书",     "Books"),
            SubCategory("stationery",            "文具",     "Stationery"),
            SubCategory("pens",                  "笔",       "Pens"),
            SubCategory("notebooks_planners",    "笔记本/手账","Notebooks & Planners"),
            SubCategory("study_aids",            "教辅",     "Study Aids"),
            SubCategory("ebooks",                "电子书",   "E-Books"),
            SubCategory("art_supplies",          "画材",     "Art Supplies"),
        ),
    ),
    "pet_supplies": GroupCategory(
        group_id="pet_supplies",
        name_zh="宠物用品",
        name_en="Pet Supplies",
        subs=(
            SubCategory("pet_cat_food",          "猫粮",     "Cat Food"),
            SubCategory("pet_dog_food",          "狗粮",     "Dog Food"),
            SubCategory("pet_treats",            "宠物零食", "Pet Treats"),
            SubCategory("pet_cat_litter",        "猫砂",     "Cat Litter"),
            SubCategory("pet_toys",              "宠物玩具", "Pet Toys"),
            SubCategory("pet_clothing",          "宠物服饰", "Pet Clothing"),
            SubCategory("pet_aquarium",          "水族用品", "Aquarium Supplies"),
            SubCategory("pet_healthcare",        "宠物医疗", "Pet Healthcare"),
        ),
    ),
    "home_improvement": GroupCategory(
        group_id="home_improvement",
        name_zh="家装建材",
        name_en="Home Improvement",
        subs=(
            SubCategory("build_tiles",           "瓷砖",     "Tiles"),
            SubCategory("build_flooring",        "地板",     "Flooring"),
            SubCategory("build_paint",           "油漆涂料", "Paint"),
            SubCategory("build_hardware",        "五金工具", "Hardware & Tools"),
            SubCategory("build_switches",        "开关插座", "Switches & Sockets"),
            SubCategory("build_plumbing",        "水管管件", "Plumbing"),
            SubCategory("build_doors_windows",   "门窗",     "Doors & Windows"),
            SubCategory("build_ceilings",        "吊顶",     "Ceilings"),
        ),
    ),
    "underwear_accessories": GroupCategory(
        group_id="underwear_accessories",
        name_zh="内衣配件",
        name_en="Underwear & Accessories",
        subs=(
            SubCategory("underwear_bras",        "文胸",     "Bras"),
            SubCategory("underwear_panties",     "内裤",     "Underwear"),
            SubCategory("underwear_socks",       "袜子",     "Socks"),
            SubCategory("underwear_leggings",    "打底裤",   "Leggings"),
            SubCategory("underwear_thermal",     "保暖内衣", "Thermal Underwear"),
            SubCategory("underwear_shapewear",   "塑身衣",   "Shapewear"),
            SubCategory("underwear_stockings",   "丝袜",     "Stockings"),
        ),
    ),
}


# ── Lookup utilities ───────────────────────────────────────────────────────────

def get_all_groups() -> list[GroupCategory]:
    """Return all group categories in definition order."""
    return list(CATEGORY_TREE.values())


def get_group_by_id(group_id: str) -> Optional[GroupCategory]:
    """Return a group category by its group_id, or None."""
    return CATEGORY_TREE.get(group_id)


def get_subs_for_group(group_id: str) -> list[SubCategory]:
    """Return all sub-categories for the given group_id."""
    group = CATEGORY_TREE.get(group_id)
    return list(group.subs) if group else []


def find_group_for_sub(sub_id: str) -> Optional[GroupCategory]:
    """Given a sub_id, return the parent GroupCategory (or None)."""
    for group in CATEGORY_TREE.values():
        for sub in group.subs:
            if sub.sub_id == sub_id:
                return group
    return None


def get_sub_by_id(sub_id: str) -> Optional[SubCategory]:
    """Return a sub-category by its sub_id, or None."""
    for group in CATEGORY_TREE.values():
        for sub in group.subs:
            if sub.sub_id == sub_id:
                return sub
    return None


def find_sub_by_platform_id(platform: str, category_id: str) -> Optional[SubCategory]:
    """Return the canonical sub-category for a platform-native category id."""
    field_map = {"taobao": "taobao_id", "tmall": "tmall_id", "1688": "id_1688"}
    attr = field_map.get(platform)
    if not attr:
        return None
    for group in CATEGORY_TREE.values():
        for sub in group.subs:
            if getattr(sub, attr) == category_id:
                return sub
    return None


def find_group_for_platform_id(platform: str, category_id: str) -> Optional[GroupCategory]:
    """
    Given a platform-native category ID (e.g. taobao_id="50014866"),
    return the parent GroupCategory, or None.
    """
    field_map = {"taobao": "taobao_id", "tmall": "tmall_id", "1688": "id_1688"}
    attr = field_map.get(platform)
    if not attr:
        return None
    for group in CATEGORY_TREE.values():
        for sub in group.subs:
            if getattr(sub, attr) == category_id:
                return group
    return None


def to_serializable_tree() -> list[dict]:
    """
    Return the full tree as a list of plain dicts suitable for JSON serialisation.

    Shape:
      [
        {
          "groupId": "womens_clothing",
          "nameZh": "女装",
          "nameEn": "Women's Clothing",
          "subCategories": [
            {"subId": "womens_dresses", "nameZh": "连衣裙", "nameEn": "Dresses",
             "taobaoId": "50014866", "tmallId": "50014866", "id1688": null},
            ...
          ]
        },
        ...
      ]
    """
    result = []
    for group in CATEGORY_TREE.values():
        result.append({
            "groupId": group.group_id,
            "nameZh": group.name_zh,
            "nameEn": group.name_en,
            "subCategories": [
                {
                    "subId":    sub.sub_id,
                    "nameZh":   sub.name_zh,
                    "nameEn":   sub.name_en,
                    "taobaoId": sub.taobao_id,
                    "tmallId":  sub.tmall_id,
                    "id1688":   sub.id_1688,
                }
                for sub in group.subs
            ],
        })
    return result
