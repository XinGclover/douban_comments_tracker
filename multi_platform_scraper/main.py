from .weibo_scraper import crawl_weibo

def main():
    targets = [
        1265743747,
        5643994130,
        1798539915
    ]

    for id in targets:
        crawl_weibo(id)


if __name__ == "__main__":
    main()
