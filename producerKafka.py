from kafka import KafkaProducer
import pandas as pd
import re
from pyvi import ViTokenizer
import spacy

nlp_ner = spacy.load("./output/model-best")


def apply_ner(text):
    doc = nlp_ner(text)
    ner_labels = [(ent.text, ent.label_) for ent in doc.ents]
    return ner_labels

KAFKA_TOPIC_NAME_CONS = "demo"
KAFKA_BOOTSTRAP_SERVERS_CONS = "192.168.1.7:9092"

def error_callback(exc):
    raise Exception("Error while sending data to Kafka: {0}".format(str(exc)))

def write_to_kafka(topic_name, items):
    count = 0
    kafka_producer_obj = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS_CONS],
        value_serializer=lambda x: x.encode("utf-8")
    )
    for _, row in items.iterrows():
        message = row.to_json()  # Convert the row to JSON format
        print(message)  # Print message to console
        kafka_producer_obj.send(topic_name, value=message).add_errback(error_callback)
        count += 1
    kafka_producer_obj.flush()
    print("Wrote {0} messages into topic: {1}".format(count, topic_name))

# Read CSV file and send to Kafka
csv_file = "./data/data.csv"

def chuyen_viet_tat(s):

    tu_dien_viet_tat = {
        r"\bmn\b": "mọi người",
        r"\bsp\b": "sản phẩm",
        r"\bđc\b": "được",
        r"\bnhah\b": "nhanh",
        r"\bhong\b": "không",
        r"\bk\b": "không",
        r"\bthui\b": "thôi",
        r"\be\b": "em",
        r"\bgh\b": "giao hàng",
        r"\bnhư v\b": "như vậy",
        r"\btr\b": "trời",
        r"\bon\b": "ổn",
        r"\bnma\b": "nhưng mà",
        r"\bnm\b": "nhưng mà",
        r"\bkhéc\b": "khét",
        r"\bhih\b": "hình",
        r"\bqc\b": "quảng cáo",
        r"\bt2\b": "thứ hai",
        r"\bsd\b": "sử dụng",
        r"\bhcm\b": "hồ chí minh",
        r"\bhn\b": "hà nội",
        r"\bcmt\b": "comment",
        r"\bng\b": "người",
        r"\bsr\b": "xin lỗi",
        r"\blg\b": "lượng",
        r"\bhsd\b": "hạn sử dụng",
        r"\bhok\b": "không",
        r"\bh\b": "giờ",
        r"\bae\b": "anh em",
        r"\bnt\b": "nhắn tin",
        r"\bsop\b": "shop",
        r"\bln\b": "luôn",
        r"\btrc\b": "trước",
        r"\bthik\b": "thích",
        r"\bmk\b": "mình",
        r"\bsx\b": "sản xuất",
        r"\bnhug\b": "nhưng",
        r"\bsl\b": "số lượng",
        r"\bthank\b": "cảm ơn",
        r"\bthanks\b": "cảm ơn",
        r"\bnka\b": "nha",
        r"\bthowm\b": "thơm",
        r"\bchuwa\b": "chưa",
        r"\bbic\b": "biết",
        r"\bgood\b": "tốt",
        r"\bgod\b": "tốt",
        r"\btn\b": "tin nhắn",
        r"\bđươcj\b": "được",
        r"\bvs\b": "với",
        r"\bdk\b": "được",
        r"\br\b": "rồi",
        r"\bmjh\b": "mình",
        r"\bntn\b": "như thế nào",
        r"\bbit\b": "biết",
        r"\bkm\b": "khuyến mãi",
        r"\bmng\b": "mọi người",
        r"\bcx\b": "cũng",
        r"\btt\b": "tiếp tục",
        r"\bcl\b": "chất lượng",
        r"\bktra\b": "kiểm tra",
        r"\bktr\b": "kiểm tra",
        r"\bz\b": "vậy",
        r"\bb\b": "bạn",
        r"\bkbt\b": "không biết",
        r"\bj\b": "gì",
        r"\bđt\b": "điện thoại",
        r"\blụong\b": "lượng",
        r"\bkaij\b": "lại",
        r"\bhk\b": "không",
        r"\bko\b": "không",
        r"\bđưj\b": "được",
        r"\bbidy\b": "body",
        r"\boki\b": "ok",
        r"\bnv\b": "nhân viên",
        r"\bchx\b": "chưa",
        r"\bbt\b": "biết",
        r"\bnhm\b": "nhưng mà",
        r"\bwa\b": "quá",
        r"\bchâm\b": "chậm",
        r"\bkhum\b": "không",
        r"\bchac\b": "chắc",
        r"\bcjhâm\b": "chậm",
        r"\bahop\b": "shop",
        r"\bxme\b": "xem",
        r"\bđnahs\b": "đánh",
        r"\bnghũ\b": "nghĩ",
        r"\btrỏng\b": "trong",
        r"\bokela\b": "ok",
        r"\btks\b": "cảm ơn",
        r"\bcùa\b": "của",
        r"\bơt\b": "ở",
        r"\bksao\b": "không sao",
        r"\bokelah\b": "ok",
        r"\bmụi\b": "mọi",
        r"\bngừoi\b": "người",
        r"\bmac\b": "mác",
        r"\bhfgng\b": "hàng",
        r"\bgg\b": "giảm giá",
        r"\bap\b": "áp",
        r"\bdc\b": "được",
        r"\bmag\b": "mang",
        r"\bbs\b": "bác sĩ",
        r"\bthươgf\b": "thường",
        r"\btu van\b": "tư vấn",
        r"\btgian\b": "thời gian",
        r"\bch\b": "chưa",
        r"\btg\b": "thời gian",
        r"\bcbi\b": "chuẩn bị",
        r"\bngưồi\b": "người",
        r"\bnhê\b": "nha",
        r"\bni\b": "này",
        r"\b\fai\b": "phải",
        r"\bship pơ\b": "shipper",
        r"\bhiêun\b": "hiệu",
        r"\btố\b": "tốt",
        r"\btuýt\b": "tuýp",
        r"\bnx\b": "nữa",
        r"\btw\b": "trung ương",
        r"\blm\b": "làm",
        r"\bqq\b": "quần què",
        r"\bzire\b": "size",
        r"\bđâh\b": "đâu",
        r"\blun\b": "luôn",
        r"\bphake\b": "fake",
        r"\blonn\b": "lol",
        r"\bchổ\b": "chỗ",
        r"\bquâ\b": "quá",
        r"\bsít\b": "shit",
        r"\bdb\b": "đầu_buồi",
        r"\bchày\b": "trầy",
        r"\bqa\b": "quá",
        r"\bns\b": "nó",
        r"\bbôg\b": "bông",
        r"\bxâu\b": "xấu",
        r"\blh\b": "liên hệ",
        r"\bll\b": "liên lạc",
        r"\bgioa\b": "giao",
        r"\bnheng\b": "nhưng",
        r"\bak\b": "á",
        r"\bphẩn\b": "phẩm",
        r"\bcám\b": "cảm",
        r"\bah\b": "á",
        r"\bokey\b": "ok",
        r"\blứm\b": "lắm",
        r"\btoẹt\b": "tuyệt",
        r"\bkím\b": "kiếm",
        r"\bgood\b": "tốt",
        r"\btq\b": "trung quốc",
        r"\btiệc vời\b": "tuyệt vời",
        r"\bcunhx\b": "cũng",
        r"\bm\b": "mình",
        r"\bspham\b": "sản phẩm",
        r"\bks\b": "không sao",
        r"\btieenf\b": "tiền",
        r"\bqt\b": "quốc tế",
        r"\bnhunge\b": "nhưng",
        r"\bphaamf\b": "phẩm",
        r"\bbk\b": "biết",
        r"\bcos\b": "có",
        r"\btot\b": "tốt",
        r"\bkh\b": "khách hàng",
        r"\bnod\b": "nó",
        r"\brui\b": "rồi",
        r"\btrk\b": "trước",
        r"\bkg\b": "không",
        r"\bđx\b": "được",
        r"\bđưocj\b": "được",
        r"\blzd\b": "lazada",
        r"\bshipper\b": "shiper",
        r"\bnhưnh\b": "nhưng",
        r"\bnhung\b": "nhưng",
        r"\bokie\b": "ok",
        r"\bokla\b": "ok",
        r"\boce\b": "ok",
        r"\bb\b": "bạn",
        r"\bph\b": "phẩm",
        r"\bsez\b": "sẽ",
        r"\bđng\b": "đóng",
        r"\bcb\b": "chuẩn bị",
        r"\bđươ\b": "được",
        r"\blượg\b": "lượng",
        r"\bđươc\b": "được",
        r"\batvs\b": "an toàn vệ sinh",
        r"\bgđ\b": "gia đình",
        r"\btc\b": "tính chất",
        r"\bđonga\b": "đóng",
        r"\bđings\b": "đóng",
        r"\bnhueng\b": "nhưng",
        r"\bre\b": "rẻ",
        r"\brats\b": "rất",
        r"\btyueet\b": "tuyệt",
        r"\bđưôcj\b": "được",
        r"\bqả\b": "quả",
        r"\bddep\b": "đẹp",
        r"\bkl\b": "không",
        r"\bnnao\b": "như nào",
        r"\bhủ\b": "hũ",
        r"\b\n\b": " ",
        r"\bgood\b": "tốt",
        r"\bbt\b": "bình thường",
        r"\bbth\b": "bình thường",
        r"\bbthg\b": "bình thường",
    }

    for viet_tat, viet_thuong in tu_dien_viet_tat.items():
        s = re.sub(viet_tat, viet_thuong, s)
    return s


def remove_stopwords(sentence):
    with open("stopword.txt", "r", encoding="utf-8") as file:
        stopword_list = file.read().splitlines()
    words = sentence.split()
    filtered_words = [word for word in words if word.lower() not in stopword_list]
    return " ".join(filtered_words)


def loai_bo_chu_cai_lap(text):
    result = re.sub(r"(\w)\1*", r"\1", text)
    return result.strip()

def tokenize_text(text):
    return ViTokenizer.tokenize(text).split()

# Function to format NER_label
def format_ner_label(row):
    if len(row['NER_label']) == 0:
        return "Unknown"
    else:
        labels = []
        for entity, tag in row['NER_label']:
            labels.append(f"{entity}: {tag}")
        return ', '.join(labels)


while True:
    try:
        data_real_time = pd.read_csv(csv_file)
        data_real_time = data_real_time.dropna()
        data_real_time['preprocessed'] = data_real_time['content'].apply(chuyen_viet_tat)
        data_real_time['preprocessed'] = data_real_time['preprocessed'].apply(remove_stopwords)
        data_real_time['preprocessed'] = data_real_time['preprocessed'].apply(loai_bo_chu_cai_lap)
        data_real_time['NER_label'] = data_real_time['content'].apply(apply_ner)
        data_real_time['NER_label'] = data_real_time.apply(format_ner_label, axis=1)
        data_real_time['tokenize'] = data_real_time['preprocessed'].apply(lambda text: ViTokenizer.tokenize(text))
        write_to_kafka(KAFKA_TOPIC_NAME_CONS, data_real_time)
    except Exception as e:
        print(f"Error: {e}")
    break  # Remove or modify this line if you want to continuously read and send data
