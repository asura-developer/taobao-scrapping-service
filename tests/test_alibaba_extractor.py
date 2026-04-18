from services.scraper_service import ScraperService, _best_alibaba_product_image, _re_extract_alibaba


def test_alibaba_extractor_prefers_current_product_image_over_previous_shop_logo():
    html = """
    <div class="search-card-e-slider">
      <div class="search-card-e-slider__wrapper">
        <a href="//www.alibaba.com/product-detail/Old-Product_1600000000001.html">
          <img class="search-card-e-slider__img" src="//s.alicdn.com/@sc04/kf/Hold.jpg_300x300.jpg">
        </a>
      </div>
    </div>
    <h2 class="search-card-e-title">
      <a href="//www.alibaba.com/product-detail/Old-Product_1600000000001.html"><span>Old product shoes</span></a>
    </h2>
    <div class="price">US$1.20</div>
    <img class="supplier-logo shop-logo" src="//oldshop.en.alibaba.com/logo.jpg">

    <div class="search-card-e-slider">
      <div class="search-card-e-slider__wrapper">
        <a href="//www.alibaba.com/product-detail/New-Product_1600000000002.html">
          <img class="search-card-e-slider__img" data-src="//s.alicdn.com/@sc04/kf/Hnew.jpg_300x300.jpg">
        </a>
      </div>
    </div>
    <h2 class="search-card-e-title">
      <a href="//www.alibaba.com/product-detail/New-Product_1600000000002.html"><span>New product shoes</span></a>
    </h2>
    <div class="price">US$2.50</div>
    """

    products = _re_extract_alibaba(html, {"keyword": "shoes"}, 1)

    assert [p["itemId"] for p in products] == ["1600000000001", "1600000000002"]
    assert products[1]["image"] == "https://s.alicdn.com/@sc04/kf/Hnew.jpg_300x300.jpg"


def test_best_alibaba_product_image_uses_lazy_attrs_before_placeholder_src():
    html = """
    <img class="search-card-e-slider__img product-main"
         src="//assets.alibaba.com/transparent.png"
         data-src="//s.alicdn.com/@sc04/kf/Hproduct.jpg_300x300.jpg">
    <img class="shop-logo" src="//supplier.en.alibaba.com/company_logo.jpg">
    """

    image = _best_alibaba_product_image(html)

    assert image == "https://s.alicdn.com/@sc04/kf/Hproduct.jpg_300x300.jpg"


def test_best_alibaba_product_image_rejects_shop_logo_when_product_image_missing():
    html = """
    <img class="supplier-logo shop-logo" alt="Supplier logo"
         src="//supplier.en.alibaba.com/company_logo.jpg">
    """

    assert _best_alibaba_product_image(html) == ""


def test_alibaba_extractor_falls_back_when_search_card_title_class_is_missing():
    html = """
    <div class="organic-gallery-offer-outter">
      <a href="//www.alibaba.com/product-detail/Factory-Unlocked-Smart-Phone_1600000000003.html"
         class="product-image-link">
        <img class="gallery product-main" data-src="//s.alicdn.com/@sc04/kf/Hphone.jpg_300x300.jpg">
      </a>
      <a href="//www.alibaba.com/product-detail/Factory-Unlocked-Smart-Phone_1600000000003.html"
         class="product-title">
        <span>Factory Unlocked Smart Phone 5G Android Mobile</span>
      </a>
      <div class="price">US$45-52</div>
    </div>
    """

    products = _re_extract_alibaba(html, {"keyword": "phone"}, 1)

    assert len(products) == 1
    assert products[0]["itemId"] == "1600000000003"
    assert products[0]["title"] == "Factory Unlocked Smart Phone 5G Android Mobile"
    assert products[0]["price"] == "45"
    assert products[0]["image"] == "https://s.alicdn.com/@sc04/kf/Hphone.jpg_300x300.jpg"
    assert products[0]["_extractionMethod"] == "alibaba_link_fallback"


def test_alibaba_block_page_detection_matches_baxia_captcha():
    html = """
    <html>
      <body class="baxia-punish captcha pc">
        <a href="https://www.alibaba.com//trade/search/_____tmd_____/page/feedback?x5secdata=abc"></a>
        <div class="nc_1_nocaptcha"><span class="slidetounlock">Slide to unlock</span></div>
      </body>
    </html>
    """

    assert ScraperService()._looks_like_alibaba_block_page(html) is True
