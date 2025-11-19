import os
import re
import scrapy


class ArxivSpider(scrapy.Spider):
    name = "arxiv"
    allowed_domains = ["arxiv.org"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        categories = os.environ.get("CATEGORIES", "cs.CV")
        categories = categories.split(",")
        # 目标分类（去空格）
        self.target_categories = set(map(str.strip, categories))
        # 为了跨分类、跨页面去重（比如同一篇同时出现在 QA 和 RT）
        self.seen_ids = set()
        # 启动页：各分类的 /new
        self.start_urls = [
            f"https://arxiv.org/list/{cat}/new" for cat in self.target_categories
        ]

    def parse(self, response):
        """
        同时遍历 #dlpage 下的所有 <dl>（包含 New submissions / Cross submissions / Replacements）
        针对每个 <dl>，按 (dt, dd) 配对解析，避免漏掉 cross-list。
        """
        # 遍历页面上所有 <dl>
        for dl in response.css("#dlpage dl"):
            dts = dl.css("dt")
            dds = dl.css("dd")
            for paper_dt, paper_dd in zip(dts, dds):
                # ---- 论文 ID ----
                # '/abs/2511.14668' or 'https://arxiv.org/abs/2511.14668'
                abs_href = paper_dt.css("a[title='Abstract']::attr(href)").get()
                if not abs_href:
                    abs_href = paper_dt.css("a[href*='/abs/']::attr(href)").get()
                if not abs_href:
                    continue
                abs_url = response.urljoin(abs_href)
                m = re.search(r"/abs/([0-9]{4}\.[0-9]{5})", abs_url)
                if not m:
                    continue
                arxiv_id = m.group(1)

                # 去重（跨分类、多次起始 URL）
                if arxiv_id in self.seen_ids:
                    continue
                # ---- 学科解析（含 cross-list）----
                # 把 .list-subjects 里所有文本拼起来，再只提取 (math.XX) 这种代码
                subject_text_parts = paper_dd.css(".list-subjects ::text").getall()
                subjects_text = " ".join(t.strip() for t in subject_text_parts if t.strip())

                # 只抓像 (math.QA)、(math.RT)、(math-ph) 这种：带点或连字符的档案代码
                # 最终保留形如 'math.QA' / 'math.RT' / 'math-ph' / 'cs.CV'
                # 注意：arXiv 学科代码统一是 'domain.DD' 或 'domain-dd'
                code_regex = r"\(([a-z\-]+\.[A-Z]{2})\)"
                categories_in_paper = re.findall(code_regex, subjects_text)

                paper_categories = set(categories_in_paper)

                # 命中任一目标分类就收
                if paper_categories.intersection(self.target_categories):
                    self.seen_ids.add(arxiv_id)
                    yield {
                        "id": arxiv_id,
                        "abs": abs_url,
                        "pdf": abs_url.replace("/abs/", "/pdf/"),
                        "categories": list(paper_categories),
                    }
                else:
                    # 有些页面极端情况下 .list-subjects 结构不标准；兜底：如果实在解析不到，就先收
                    if not subjects_text:
                        self.logger.warning(
                            f"Could not extract categories for paper {arxiv_id}, including anyway"
                        )
                        self.seen_ids.add(arxiv_id)
                        yield {
                            "id": arxiv_id,
                            "abs": abs_url,
                            "pdf": abs_url.replace("/abs/", "/pdf/"),
                            "categories": [],
                        }
                    # 否则正常跳过
                    else:
                        self.logger.debug(
                            f"Skipped {arxiv_id} with categories {paper_categories} "
                            f"(target: {self.target_categories})"
                        )
