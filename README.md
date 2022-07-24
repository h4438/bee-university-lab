# bee-university
> Project Open Source provide by BeeCost and Metric


Mục tiêu Project:

+ Thu thập gần 1 triệu dữ liệu điểm thi THPT Quốc gia năm 2019, 2020, 2021
 
+ Thu thập điểm chuẩn đại học 2014 - 2020

+ Phân tích dữ liệu

Hãy bấm Star để ủng hộ BeeCost nhé :kissing_heart:

  

# Development

```bash
# OS Any (Ubuntu - Recommend)

#install python 
# detail: deploy/docs/install_python.md

#install python 3.7
# detail: deploy/docs/install_python37.md


mkdir -p /bee_university

git clone https://github.com/beecost/bee-university.git
cd bee-university
git config credential.helper store
 

virtualenv venv -p python3.7
source venv/bin/activate
pip install -r requirements.txt

# Update folder path in init.py
python init.py
python init_server.py

# Crawl danh sách url trường đại học
python crawler/crawler_university_list.py
# Crawl điểm chuẩn từ 2014 - 2019
python crawler/crawl_diemchuan.py
# Crawl điểm thi 2020 64 tỉnh thành
python crawler/diemthi2020/crawler_diemthi2020.py
```

# Output

> /bee_university/crawler/common/university.gz


1. Download: [university.gz](https://github.com/beecost/bee-university/blob/master/output_data/crawler/common/university.gz) 

> /bee_university/crawler/common/university_diemchuan.gz

2. Download: [university_diemchuan.gz](https://github.com/beecost/bee-university/blob/master/output_data/crawler/common/university_diemchuan.gz) 

> /bee_university/crawler/common/diemthi_2020/provide_{code}_{part}.gz

3. Full dữ liệu điểm thi THPT Quốc gia

**Dữ liệu điểm thi THPT năm 2021**

Format csv : [diemthi2021.csv.zip](https://github.com/beecost/bee-university/blob/master/output_data/crawler/common/diemthi_2021_transform/diemthi2021.csv.zip)

**Dữ liệu điểm thi THPT năm 2020**

Format csv : [diemthi2020.csv.zip](https://github.com/beecost/bee-university/blob/master/output_data/crawler/common/diemthi_2020_transform/diemthi2020.csv.zip)

**Dữ liệu điểm thi THPT năm 2019**

Format csv : [diemthi2019.csv.zip](https://github.com/beecost/bee-university/blob/master/output_data/crawler/common/diemthi_2019_transform/diemthi2019.csv.zip)

Download [BeeCost Extension](https://www.beecost.com/download/extension?pub=github_opensource) (ủng hộ BeeCost <3)

## University điểm chuẩn 2014 - 2019
File: `university_diemchuan.gz`

```javascript
{"diemchuan_datas": [{"major_code": "CN1", "major_name": "Công nghệ Thông tin", "subject_group": "A00; A01; D07", "point": "23.75", "note": "", "year": 2018}, {"major_code": "CN2", "major_name": "Máy tính và Robot", "subject_group": "A00; A01; D07", "point": "21", "note": "", "year": 2018}, {"major_code": "CN3", "major_name": "Vật lý kỹ thuật", "subject_group": "A00; A01; D07", "point": "18.75", "note": "", "year": 2018}, {"major_code": "CN4", "major_name": "Cơ kỹ thuật", "subject_group": "A00; A01; D07", "point": "20.5", "note": "", "year": 2018}, {"major_code": "CN5", "major_name": "Công nghệ kỹ thuật xây dựng", "subject_group": "A00; A01; D07", "point": "18", "note": "", "year": 2018}, {"major_code": "CN6", "major_name": "Công nghệ kỹ thuật cơ điện tử", "subject_group": "A00; A01; D07", "point": "22", "note": "", "year": 2018}, {"major_code": "CN7", "major_name": "Công nghệ Hàng không vũ trụ", "subject_group": "A00; A01; D07", "point": "19", "note": "", "year": 2018}, {"major_code": "CN8", "major_name": "Khoa học máy tính", "subject_group": "A00; A01; D07", "point": "22", "note": "", "year": 2018}, {"major_code": "CN9", "major_name": "Công nghệ kỹ thuật điện tử - viễn thông", "subject_group": "A00; A01; D07", "point": "20", "note": "", "year": 2018}, {"major_code": "CN1", "major_name": "Công nghệ Thông tin", "subject_group": "A00; A01; D07", "point": "26", "note": "", "year": 2017}, {"major_code": "CN2", "major_name": "Máy tính và Robot", "subject_group": "A00; A01; D07", "point": "---", "note": "", "year": 2017}, {"major_code": "CN3", "major_name": "Vật lý kỹ thuật", "subject_group": "A00; A01; D07", "point": "19", "note": "", "year": 2017}, {"major_code": "CN4", "major_name": "Cơ kỹ thuật", "subject_group": "A00; A01; D07", "point": "23.5", "note": "", "year": 2017}, {"major_code": "CN5", "major_name": "Công nghệ kỹ thuật xây dựng", "subject_group": "A00; A01; D07", "point": "23.5", "note": "", "year": 2017}, {"major_code": "CN6", "major_name": "Công nghệ kỹ thuật cơ điện tử", "subject_group": "A00; A01; D07", "point": "23.5", "note": "", "year": 2017}, {"major_code": "CN7", "major_name": "Công nghệ Hàng không vũ trụ", "subject_group": "A00; A01; D07", "point": "---", "note": "", "year": 2017}, {"major_code": "CN8", "major_name": "Khoa học máy tính", "subject_group": "A00; A01; D07", "point": "26", "note": "", "year": 2017}, {"major_code": "CN9", "major_name": "Công nghệ kỹ thuật điện tử - viễn thông", "subject_group": "A00; A01; D07", "point": "26", "note": "", "year": 2017}, {"major_code": "QHITD2", "major_name": "Công nghệ kỹ thuật Xây dựng-Giao thông", "subject_group": "A00; A02", "point": "---", "note": "", "year": 2016}, {"major_code": "QHITD1", "major_name": "Kỹ thuật năng lượng", "subject_group": "A00; A02", "point": "81", "note": "", "year": 2016}, {"major_code": "7520401", "major_name": "Vật lý kỹ thuật", "subject_group": "A00; A02", "point": "87", "note": "", "year": 2016}, {"major_code": "7520214", "major_name": "Kỹ thuật máy tính", "subject_group": "A00; A02", "point": "---", "note": "", "year": 2016}, {"major_code": "7520101", "major_name": "Cơ kỹ thuật", "subject_group": "A00; A02", "point": "87", "note": "", "year": 2016}, {"major_code": "7510302CLC", "major_name": "Công nghệ kỹ thuật điện tử, truyền thông (CLC)", "subject_group": "A01; D07; D08", "point": "125", "note": "", "year": 2016}, {"major_code": "7510302", "major_name": "Công nghệ kỹ thuật điện tử, truyền thông", "subject_group": "A00; A02", "point": "95", "note": "", "year": 2016}, {"major_code": "7510203", "major_name": "Công nghệ kỹ thuật cơ điện tử", "subject_group": "A00; A02", "point": "94", "note": "", "year": 2016}, {"major_code": "7480201NB", "major_name": "Công nghệ Thông tin định hướng thị trường Nhật Bản", "subject_group": "A00; A02", "point": "---", "note": "", "year": 2016}, {"major_code": "7480201", "major_name": "Công nghệ thông tin", "subject_group": "A00; A02", "point": "103", "note": "", "year": 2016}, {"major_code": "7480104", "major_name": "Hệ thống thông tin", "subject_group": "A00; A02", "point": "98", "note": "", "year": 2016}, {"major_code": "7480102", "major_name": "Truyền thông và mạng máy tính", "subject_group": "A00; A02", "point": "98", "note": "", "year": 2016}, {"major_code": "7480101CLC", "major_name": "Khoa học Máy tính (CLC)", "subject_group": "A01; D07; D08", "point": "125", "note": "", "year": 2016}, {"major_code": "7480101", "major_name": "Khoa học máy tính", "subject_group": "A00; A02", "point": "98", "note": "", "year": 2016}, {"major_code": "7480201", "major_name": "Công nghệ thông tin", "subject_group": "", "point": "109", "note": "", "year": 2015}, {"major_code": "7480101", "major_name": "Khoa học máy tính", "subject_group": "", "point": "106.5", "note": "", "year": 2015}, {"major_code": "7480104", "major_name": "Hệ thống thông tin", "subject_group": "", "point": "106.5", "note": "", "year": 2015}, {"major_code": "7480102", "major_name": "Truyền thông và mạng máy tính", "subject_group": "", "point": "106.5", "note": "", "year": 2015}, {"major_code": "7510302", "major_name": "Công nghệ kĩ thuật điện tử, truyền thông", "subject_group": "", "point": "102.5", "note": "", "year": 2015}, {"major_code": "7D0401", "major_name": "Vật lí kĩ thuật", "subject_group": "", "point": "91.5", "note": "", "year": 2015}, {"major_code": "7D0101", "major_name": "Cơ kĩ thuật", "subject_group": "", "point": "97.5", "note": "", "year": 2015}, {"major_code": "7510203", "major_name": "Công nghệ kĩ thuật cơ điện tử", "subject_group": "", "point": "99.5", "note": "", "year": 2015}, {"major_code": "7480201", "major_name": "Công nghệ thông tin", "subject_group": "A", "point": "22", "note": "", "year": 2014}, {"major_code": "7480201", "major_name": "Công nghệ thông tin", "subject_group": "A1", "point": "22", "note": "", "year": 2014}, {"major_code": "7480101", "major_name": "Khoa học máy tính", "subject_group": "A", "point": "22", "note": "", "year": 2014}, {"major_code": "7480101", "major_name": "Khoa học máy tính", "subject_group": "A1", "point": "22", "note": "", "year": 2014}, {"major_code": "7480104", "major_name": "Hệ thống thông tin", "subject_group": "A", "point": "22", "note": "", "year": 2014}, {"major_code": "7480104", "major_name": "Hệ thống thông tin", "subject_group": "A1", "point": "22", "note": "", "year": 2014}, {"major_code": "7480102", "major_name": "Truyền thông và mạng máy tính", "subject_group": "A", "point": "22", "note": "", "year": 2014}, {"major_code": "7480102", "major_name": "Truyền thông và mạng máy tính", "subject_group": "A1", "point": "22", "note": "", "year": 2014}, {"major_code": "7510302", "major_name": "Công nghệ kỹ thuật điện tử, truyền thông", "subject_group": "A", "point": "19.5", "note": "", "year": 2014}, {"major_code": "7510302", "major_name": "Công nghệ kỹ thuật điện tử, truyền thông", "subject_group": "A1", "point": "19.5", "note": "", "year": 2014}, {"major_code": "7520401", "major_name": "Vật lý kỹ thuật", "subject_group": "A", "point": "18", "note": "", "year": 2014}, {"major_code": "7510203", "major_name": "Công nghệ kỹ thuật cơ điện tử", "subject_group": "A", "point": "18", "note": "", "year": 2014}, {"major_code": "7520101", "major_name": "Cơ kỹ thuật", "subject_group": "A", "point": "18", "note": "", "year": 2014}], "university_meta": {"url": "https://diemthi.tuyensinh247.com/diem-chuan/dai-hoc-cong-nghe-dai-hoc-quoc-gia-ha-noi-QHI.html", "university_code": "QHI", "university_name": "Đại Học Công Nghệ – Đại Học Quốc Gia Hà Nội"}}
{"diemchuan_datas": [{"major_code": "7380101", "major_name": "Luật", "subject_group": "C00", "point": "24.5", "note": "", "year": 2018}, {"major_code": "7380101", "major_name": "Luật", "subject_group": "A00", "point": "18.5", "note": "", "year": 2018}, {"major_code": "7380101", "major_name": "Luật", "subject_group": "D01", "point": "18.5", "note": "", "year": 2018}, {"major_code": "7380101", "major_name": "Luật", "subject_group": "D03", "point": "18", "note": "", "year": 2018}, {"major_code": "7380101", "major_name": "Luật", "subject_group": "D78", "point": "19", "note": "", "year": 2018}, {"major_code": "7380101", "major_name": "Luật", "subject_group": "D82", "point": "19", "note": "", "year": 2018}, {"major_code": "7380101 CLC", "major_name": "Luật Chất lượng cao", "subject_group": "A01; D01; D07; D78", "point": "18.25", "note": "", "year": 2018}, {"major_code": "7380110", "major_name": "Luật kinh doanh", "subject_group": "A00; A01; D01; D03; D78; D82", "point": "20.75", "note": "", "year": 2018}, {"major_code": "7380109", "major_name": "Luật Thương mại Quốc tế", "subject_group": "A00; A01; D01; D03; D78; D82", "point": "---", "note": "", "year": 2018}, {"major_code": "", "major_name": "Các ngành đào tạo đại học", "subject_group": "", "point": "---", "note": "", "year": 2017}, {"major_code": "7380101", "major_name": "Luật", "subject_group": "A00; C00; D01; D03; D78; D82", "point": "27.25", "note": "", "year": 2017}, {"major_code": "7380101CLC", "major_name": "Luật chất lượng cao đáp ứng Thông tư 23", "subject_group": "A01; D01; D07; D07; D78", "point": "---", "note": "", "year": 2017}, {"major_code": "7380110", "major_name": "Luật kinh doanh", "subject_group": "A00; A01; D01; D03; D78; D82", "point": "24", "note": "", "year": 2017}, {"major_code": "7380109", "major_name": "Luật kinh doanh*", "subject_group": "A00; D01; D02; D03", "point": "---", "note": "", "year": 2016}, {"major_code": "7380101", "major_name": "Luật", "subject_group": "C00; D01; D02; D03", "point": "---", "note": "", "year": 2016}, {"major_code": "7380101", "major_name": "Luật học", "subject_group": "", "point": "100.5", "note": "", "year": 2015}, {"major_code": "7380109", "major_name": "Luật kinh doanh", "subject_group": "", "point": "103", "note": "", "year": 2015}, {"major_code": "7380101", "major_name": "Luật học", "subject_group": "A", "point": "20", "note": "", "year": 2014}, {"major_code": "7380101", "major_name": "Luật học", "subject_group": "A1", "point": "20", "note": "", "year": 2014}, {"major_code": "7380101", "major_name": "Luật học", "subject_group": "C", "point": "20", "note": "", "year": 2014}, {"major_code": "7380101", "major_name": "Luật học", "subject_group": "D1", "point": "20", "note": "", "year": 2014}, {"major_code": "7380101", "major_name": "Luật học", "subject_group": "D3", "point": "20.5", "note": "", "year": 2014}, {"major_code": "7380109", "major_name": "Luật kinh doanh", "subject_group": "A", "point": "22", "note": "", "year": 2014}, {"major_code": "7380109", "major_name": "Luật kinh doanh", "subject_group": "A1", "point": "22", "note": "", "year": 2014}, {"major_code": "7380109", "major_name": "Luật kinh doanh", "subject_group": "D1", "point": "21.5", "note": "", "year": 2014}, {"major_code": "7380109", "major_name": "Luật kinh doanh", "subject_group": "D3", "point": "21.5", "note": "", "year": 2014}], "university_meta": {"url": "https://diemthi.tuyensinh247.com/diem-chuan/khoa-luat-dai-hoc-quoc-gia-ha-noi-QHL.html", "university_code": "QHL", "university_name": "Khoa Luật – Đại Học Quốc Gia Hà Nội"}}
```

## Điểm thi THPT Quốc gia 2020

Folder: `/bee_university/crawler/common/diemthi_2020`

```javascript

{"sbd": "01000887", "Toan": 5.6, "Van": 6.25, "Su": 6.0, "Đia": 6.25, "GDCD": 8.75, "Ngoai_ngu": 7.8, "Ma_mon_ngoai_ngu": "N1"}
{"sbd": "01000889", "Toan": 7.6, "Van": 5.5, "Su": 8.5, "Đia": 8.25, "GDCD": 8.5, "Ngoai_ngu": 4.6, "Ma_mon_ngoai_ngu": "N1"}
{"sbd": "01000886", "Toan": 4.2, "Van": 5.25, "Su": 5.5, "Đia": 7.5, "GDCD": 6.75, "Ngoai_ngu": 3.0, "Ma_mon_ngoai_ngu": "N1"}
{"sbd": "01000890", "Toan": 2.8, "Van": 4.5, "Su": 3.25, "Đia": 5.75, "GDCD": 7.0, "Ngoai_ngu": 4.8, "Ma_mon_ngoai_ngu": "N1"}
{"sbd": "01000922", "Toan": 4.8, "Van": 6.0, "Li": 2.5, "Hoa": 2.25, "Sinh": 3.5, "Su": 2.5, "Đia": 4.5, "Ngoai_ngu": 4.4, "Ma_mon_ngoai_ngu": "N1"}
{"sbd": "01000923", "Toan": 3.8, "Van": 3.5, "Li": 2.5, "Hoa": 2.25, "Sinh": 2.0, "Su": 3.5, "Đia": 6.25}
{"sbd": "01000929", "Toan": 5.6, "Van": 6.25, "Li": 3.5, "Hoa": 2.25, "Sinh": 3.5, "Su": 4.0, "Đia": 5.75}
```

# Stack

Python

Numpy

Pandas

Spark


# BeeCost - Tiện ích mua sắm Online

[BeeCost.VN](https://beecost.vn), [BeeCost.Com](https://www.beecost.com) là Trợ lý mua sắm online. Giúp bạn mua hàng tiết kiệm hơn trên Shopee, Tiki, Sendo, Lazada, Adayroi. Ứng dụng được tạo từ việc phân tích hơn 50 triệu sản phẩm thương mại điện tử mỗi ngày. 

Tính năng chính của tiện ích BeeCost:

- Lịch sử giá hơn 50 triệu sản phẩm
- So sánh giá tìm nơi bán rẻ nhất
- Price Alert (Thông báo khi giảm giá)
- Tìm kiếm mã giảm giá tự động

> Tìm hiểu BeeCost tại [beecost.com](https://www.beecost.com) và [beecost.vn](https://beecost.vn)

> Download [BeeCost Extension](https://www.beecost.com/download/extension?pub=github_opensource) trên Google Chrome