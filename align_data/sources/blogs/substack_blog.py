from align_data.common.html_dataset import RSSDataset


class SubstackBlog(RSSDataset):
    source_type = "substack"
    date_format = "%a, %d %b %Y %H:%M:%S %Z"

    @property
    def feed_url(self):
        return self.url + "/feed"
