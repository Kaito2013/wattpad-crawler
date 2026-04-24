# Wattpad Young Adult Crawler

Crawl truyện Young Adult từ Wattpad sử dụng API v4.

## Features

- API-based crawler (không cần browser)
- Chỉ lấy **completed stories**
- Rate limit friendly (1.5-3s delay)
- Auto-save partial results
- Exponential backoff khi bị block

## Fields

| Column | Mô tả |
|--------|--------|
| Rank | Thứ hạng theo lượt đọc |
| URL | Link truyện trên Wattpad |
| Title | Tên truyện |
| Reads | Lượt đọc |
| Votes | Lượt vote |
| Status | Trạng thái (Completed) |

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
# Crawl 100 stories
python wattpad_api_crawler.py --max-stories 100

# Crawl 500 stories, save to custom file
python wattpad_api_crawler.py --max-stories 500 --output my_output.xlsx

# Crawl 1000 stories
python wattpad_api_crawler.py --max-stories 1000
```

## Output

File Excel trong thư mục `output/`

## Notes

- Wattpad API limit: ~50 stories/request
- ~105,574 stories Young Adult available
- Respect rate limit để tránh bị block

## License

MIT
