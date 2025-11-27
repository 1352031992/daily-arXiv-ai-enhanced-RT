import os
import re
import scrapy


class ArxivSpider(scrapy.Spider):
    name = "arxiv"
    allowed_domains = ["arxiv.org"]

    # 为了保证页面按顺序处理（比如 math.QA 再 math.RT）
    custom_settings = {
        "CONCURRENT_REQUESTS": 1
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # 实际抓取的分类完全由环境变量 CATEGORIES 决定，逗号分隔：
        #   CATEGORIES="math.RT"
        #   CATEGORIES="math.QA,math.RT"
        # 如果没有设置或是空字符串，就默认用 cs.CV
        categories_str = os.environ.get("CATEGORIES", "").strip()
        if not categories_str:
            categories_str = "cs.CV"

        cats = [c.strip() for c in categories_str.split(",") if c.strip()]

        # 分类优先级：如果你用 math.QA, math.RT，就会先处理 QA 再 RT；
        # 其他分类都排在后面（优先级 99）
        self.CAT_PRIORITY = {"math.QA": 0, "math.RT": 1}
        cats.sort(key=lambda c: self.CAT_PRIORITY.get(c, 99))

        self.target_categories = set(cats)
        self.start_urls = [f"https://arxiv.org/list/{cat}/new" for cat in cats]

        # 全局去重，避免 QA/RT/其它分类交叉时同一篇重复
        self.seen_ids = set()

    def parse(self, response):
        """
        需求：
        1) 分类之间按优先级排序（由 __init__ + CONCURRENT_REQUESTS=1 保证页面处理顺序）
        2) 每个分类内：New submissions -> Cross submissions -> Replacements
        3) 同层内：按 arXiv 编号倒序
        """
        # 从当前 URL 提取“来源分类”，用于分类优先级
        # 形如 https://arxiv.org/list/math.QA/new
        mcat = re.search(r"/list/([^/]+)/new", response.url)
        source_cat = mcat.group(1) if mcat else ""
        cat_priority = self.CAT_PRIORITY.get(source_cat, 99)

        page_items = []

        # 遍历 #dlpage 下 h3/dl 的交替结构，识别区块标题
        # 使用 xpath 保证顺序：h3 -> dl -> h3 -> dl ...
        current_section_rank = 3  # 默认未知区块
        for section in response.xpath("//div[@id='dlpage']/*[self::h3 or self::dl]"):
            tag = section.root.tag.lower()

            # 识别区块类型，映射成排序键
            if tag == "h3":
                heading = "".join(section.css("::text").getall()).strip().lower()
                if "new submission" in heading:
                    current_section_rank = 0
                elif "cross submission" in heading:
                    current_section_rank = 1
                elif "replacement" in heading:
                    current_section_rank = 2
                else:
                    current_section_rank = 3
                continue

            if tag != "dl":
                continue

            # 逐条解析该区块里的 dt/dd
            dts = section.css("dt")
            dds = section.css("dd")
            for paper_dt, paper_dd in zip(dts, dds):
                # ---- arXiv id ----
                abs_href = paper_dt.css("a[title='Abstract']::attr(href)").get()
                if not abs_href:
                    abs_href = paper_dt.css("a[href*='/abs/']::attr(href)").get()
                if not abs_href:
                    continue

                abs_url = response.urljoin(abs_href)
                mid = re.search(r"/abs/([0-9]{4}\.[0-9]{5})", abs_url)
                if not mid:
                    continue
                arxiv_id = mid.group(1)

                # 去重（跨分类/跨区块）
                if arxiv_id in self.seen_ids:
                    continue

                # ---- 学科解析（包含 cross-list）----
                subj_parts = paper_dd.css(".list-subjects ::text").getall()
                subjects_text = " ".join(t.strip() for t in subj_parts if t.strip())

                # 只提取学科代码，如 (math.QA)、(math.RT)、(math-ph)、(cs.CV)
                code_regex = r"\(([a-z\-]+\.[A-Z]{2})\)"
                categories_in_paper = re.findall(code_regex, subjects_text)
                paper_categories = set(categories_in_paper)

                # ===== 是否命中目标分类的逻辑 =====
                # 1. 正则命中目标分类
                has_target = bool(paper_categories.intersection(self.target_categories))

                # 2. 如果正则没命中，但当前页面本身就是某个目标分类，
                #    仍然认为命中，避免因为解析失败漏掉论文
                if not has_target and source_cat in self.target_categories:
                    has_target = True
                    if source_cat:
                        paper_categories.add(source_cat)

                if has_target:
                    self.seen_ids.add(arxiv_id)
                    page_items.append({
                        "id": arxiv_id,
                        "abs": abs_url,
                        "pdf": abs_url.replace("/abs/", "/pdf/"),
                        "categories": list(paper_categories),
                        # 排序键
                        "cat_priority": cat_priority,
                        "section_rank": current_section_rank,
                    })
                else:
                    if not subjects_text:
                        # 兜底：极少数结构异常，仍然收录，放在最末区块
                        self.logger.warning(
                            f"Could not extract categories for paper {arxiv_id}, including anyway"
                        )
                        self.seen_ids.add(arxiv_id)
                        page_items.append({
                            "id": arxiv_id,
                            "abs": abs_url,
                            "pdf": abs_url.replace("/abs/", "/pdf/"),
                            "categories": [],
                            "cat_priority": cat_priority,
                            "section_rank": 3,
                        })
                    else:
                        # 真正被过滤掉的情况，这里打印详细信息方便排查
                        self.logger.debug(
                            f"Skipped {arxiv_id} on page {source_cat} "
                            f"with parsed categories {paper_categories} "
                            f"(target: {self.target_categories}), "
                            f"subjects_text={subjects_text!r}"
                        )

        # ===== 排序 =====
        # 规则：分类优先级(升) -> 区块(New=0, Cross=1, Replacements=2, 其余=3)(升) -> arXiv编号(降)
        # 用稳定排序实现：先按 id 降序，再按 section 升序，再按分类升序
        page_items.sort(key=lambda x: x["id"], reverse=True)
        page_items.sort(key=lambda x: x["section_rank"])
        page_items.sort(key=lambda x: x["cat_priority"])

        # 输出时去掉临时键
        for it in page_items:
            it.pop("cat_priority", None)
            it.pop("section_rank", None)
            yield it
