import os
import re
import scrapy


class ArxivSpider(scrapy.Spider):
    name = "arxiv"
    allowed_domains = ["arxiv.org"]

    # 为了让 QA 页先抓、再抓 RT 页（避免并发打乱全局顺序）
    custom_settings = {
        "CONCURRENT_REQUESTS": 1
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # 用环境变量 CATEGORIES 决定要抓哪些分类
        # 例如：CATEGORIES="math.RT"  或  CATEGORIES="math.QA,math.RT"
        categories = os.environ.get("CATEGORIES", "cs.CV")
        cats = [c.strip() for c in categories.split(",") if c.strip()]

        # 分类优先级：QA 在 RT 前（只是为了最终排序好看一点）
        self.CAT_PRIORITY = {
            "math.QA": 0,
            "math.RT": 1,
        }

        # 按优先级排序 start_urls（未知分类排后面）
        cats.sort(key=lambda c: self.CAT_PRIORITY.get(c, 99))

        self.target_categories = set(cats)  # 现在只用于日志，不再做过滤
        self.start_urls = [f"https://arxiv.org/list/{cat}/new" for cat in cats]

        # 全局去重，避免同一篇在 QA/RT 都出现
        self.seen_ids = set()

    def parse(self, response):
        """
        现在的策略：

        - 你用 CATEGORIES 决定要抓哪些 /list/<cat>/new 页面；
        - 对每个这样的页面，**该页上的所有 paper 都收**（不再按 Subjects 过滤）；
        - Subjects 仅用于填充 `categories` 字段，防止因为 arXiv 的 HTML 细微差异而漏抓。
        """

        # 从当前 URL 提取来源分类，例如 https://arxiv.org/list/math.RT/new
        mcat = re.search(r"/list/([^/]+)/new", response.url)
        source_cat = mcat.group(1) if mcat else ""
        cat_priority = self.CAT_PRIORITY.get(source_cat, 99)

        page_items = []
        current_section_rank = 3  # 默认其他区块

        # 遍历 #dlpage 下 h3/dl 的交替结构，识别区块标题
        # 使用 xpath 保证顺序：h3 -> dl -> h3 -> dl ...
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

            # 每个 dl 下面是一组 dt / dd
            dts = section.css("dt")
            dds = section.css("dd")

            for paper_dt, paper_dd in zip(dts, dds):
                # 先拿到 abs 链接
                abs_href = paper_dt.css("a[title='Abstract']::attr(href)").get()
                if not abs_href:
                    abs_href = paper_dt.css("a[href*='/abs/']::attr(href)").get()
                if not abs_href:
                    continue

                abs_url = response.urljoin(abs_href)

                # 从链接里抽 arXiv id（不带版本号）
                mid = re.search(r"/abs/([0-9]{4}\.[0-9]{5})", abs_url)
                if not mid:
                    continue
                arxiv_id = mid.group(1)

                # 全局去重：如果之前某个分类已经收过这一篇，就跳过
                if arxiv_id in self.seen_ids:
                    continue
                self.seen_ids.add(arxiv_id)

                # ---- 解析 Subjects 用来填充 categories（但不再做过滤） ----
                subj_parts = paper_dd.css(".list-subjects ::text").getall()
                subjects_text = " ".join(t.strip() for t in subj_parts if t.strip())

                # 提取学科代码，如 (math.RT)、(math.QA)、(cs.CV) 等
                code_regex = r"\(([a-z\-]+\.[A-Z]{2})\)"
                categories_in_paper = re.findall(code_regex, subjects_text)
                paper_categories = set(categories_in_paper)

                if not subjects_text:
                    # 极少数结构异常的情况，打个 warning 但仍然收录
                    self.logger.warning(
                        f"Could not extract categories for paper {arxiv_id}, "
                        f"source page = {source_cat}"
                    )

                page_items.append(
                    {
                        "id": arxiv_id,
                        "abs": abs_url,
                        "pdf": abs_url.replace("/abs/", "/pdf/"),
                        "categories": list(paper_categories),
                        # 排序用的临时键
                        "cat_priority": cat_priority,
                        "section_rank": current_section_rank,
                    }
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
