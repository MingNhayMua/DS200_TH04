# TH04 - Phân Tích Dữ Liệu Thương Mại Điện Tử Bằng PySpark

## Yêu cầu hệ thống

| Thành phần | Phiên bản |
|------------|-----------|
| Python     | 3.8+ |
| PySpark    | 3.5.x |
| Java JDK   | 8/11/17 |

## Cài đặt

```bash
pip install pyspark
```

## Cấu trúc dự án

```
TH04_PySpark/
├── data/                    # Symlink -> ../TH04/data/
├── result/                  # Kết quả đầu ra
├── bai01.py ~ bai10.py      # Mã nguồn từng bài
└── README.md
```

## Chạy từng bài

```bash
# Chạy và xem kết quả trên terminal
python bai01.py

# Chạy và lưu kết quả vào file
python bai01.py > result/bai01.txt 2>&1
```

## Chạy tất cả và lưu kết quả

```bash
# Tạo file kết quả cho tất cả các bài
python bai01.py > result/bai01.txt 2>&1
python bai02.py > result/bai02.txt 2>&1
python bai03.py > result/bai03.txt 2>&1
python bai04.py > result/bai04.txt 2>&1
python bai05.py > result/bai05.txt 2>&1
python bai06.py > result/bai06.txt 2>&1
python bai07.py > result/bai07.txt 2>&1
python bai08.py > result/bai08.txt 2>&1
python bai09.py > result/bai09.txt 2>&1
python bai10.py > result/bai10.txt 2>&1
```

Hoặc dùng script:

```bash
for i in {01..10}; do
    echo "Dang chay bai$i..."
    python bai$i.py > result/bai$i.txt 2>&1
    echo "  -> $(wc -l < result/bai$i.txt) dong"
done
```

---

## Mô tả các bài

### Câu 1-5 (Bắt buộc)

| Bài | Nội dung | File |
|-----|----------|------|
| 1 | Đọc 5 file CSV, in schema và 5 dòng đầu | `bai01.py` |
| 2 | Thống kê tổng đơn hàng, khách hàng, người bán | `bai02.py` |
| 3 | Đơn hàng theo quốc gia (giảm dần) | `bai03.py` |
| 4 | Đơn hàng theo năm, tháng (năm tăng, tháng giảm) | `bai04.py` |
| 5 | Điểm đánh giá TB & theo mức, xử lý NULL | `bai05.py` |

### Câu 6-10 (Chọn 3)

| Bài | Nội dung | File |
|-----|----------|------|
| 6 | Doanh thu 2024 theo danh mục sản phẩm | `bai06.py` |
| 7 | Sản phẩm bán chạy & điểm đánh giá TB | `bai07.py` |
| 8 | Hiệu suất giao hàng | `bai08.py` |
| 9 | Phân nhóm khách hàng | `bai09.py` |
| 10 | Xếp hạng seller | `bai10.py` |
