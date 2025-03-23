import sys
from typing import List, Dict, Generator, IO, Optional
from collections import namedtuple
import logging
import gzip
import os
import json
import re
import structlog
import statistics

config: Dict = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log", }
LogFile = namedtuple("LogFile", ["name", "date", "extention"])
ParserOutput = namedtuple("ParserOutput", ["entries", "total"])


def log_dir_exists(dir_path: Optional[str]) -> bool:
    log = structlog.get_logger()
    log.info(message="Cheking log dir exists", dir_path=dir_path)
    if not dir_path:
        log.info(
            message="Configured log directory path is empty or None, returned False",
            dir_path=dir_path,
        )
        return False
    return os.path.exists(dir_path)


def get_log_files(dir_path: Optional[str]) -> List[str]:
    log = structlog.get_logger()
    log.info(message="Getting log dir list", dir_path=dir_path)
    if not dir_path:
        log.info(
            message="Configured log directory path is empty or None, returned []",
            dir_path=dir_path,
        )
        return []
    try:
        return os.listdir(dir_path)
    except FileNotFoundError:
        log.error(
            message="Failed to get logs list: directory not found", dir_path=dir_path
        )
        return []
    except NotADirectoryError:
        log.error(message="Failed to get logs list: not a directory", dir_path=dir_path)
        return []


def search_latest(log_files: List[str]) -> LogFile:
    log = structlog.get_logger()
    log.info(message="Search latest log file", log_files=log_files)
    search_pattern = r"^nginx-access-ui\.log-(?P<date>\d{8})(?:\.gz)?$"
    log.info(message="Configure search regex pattern", search_pattern=search_pattern)
    latest_date = "00000000"
    latest_log = ""
    for log_name in log_files:
        name_match = re.search(search_pattern, log_name)
        if name_match:
            date = name_match.group("date")
            log.info(message="Name of log file matched", log_name=log_name, date=date)

            if date > latest_date:
                log.info(
                    message="Log file date greater, than previous, update latest values",
                    latest_log=latest_log,
                    latest_date=date,
                )
                latest_date = date
                latest_log = log_name
                log.info(
                    message="Latest values updated",
                    latest_log=latest_log,
                    latest_date=date,
                )
    log.info(
        message="Searching finished", latest_log=latest_log, latest_date=latest_date
    )
    return LogFile(latest_log, latest_date, ".gz" if ".gz" in latest_log else ".log")


def get_log_path(log_name: str, log_dir: Optional[str]) -> Optional[str]:
    log = structlog.get_logger()
    log.info(message="Trying to get log path", log_name=log_name, log_dir=log_dir)
    if not log_dir:
        log.info(message='Log dir path is empty or None, returned ""', log_dir=log_dir)
        return None
    if not log_name:
        log.info(message='Log name is empty or None, returned ""', log_name=log_name)
        return None
    return f"{log_dir}/{log_name}"


def entries_parser(log_file: IO[str]) -> Generator[Dict, None, None]:
    log = structlog.get_logger()
    parsing_pattern = r"(?:GET|POST|PUT|DELETE|HEAD|OPTIONS|PATCH)\s+(?P<url>[^\s]+).*?\s+(?P<request_time>\d+\.\d+)$"
    log.info(message="Configure parsing pattern", parsing_patter=parsing_pattern)
    idx = 0
    for line in log_file:
        idx += 1
        line_match = re.search(parsing_pattern, line)
        if not line_match:
            log.error(
                message="Failed to parse line",
                log_file=log_file.name,
                line_index=idx,
                line=line,
            )
            yield {}
        else:
            yield line_match.groupdict()


def parse_entries(parser: Generator[Dict, None, None]) -> ParserOutput:
    log = structlog.get_logger()
    log.info(message="Starting log entries parsing")
    entries: Dict = {}
    total: Dict = {"entries": 0, "request_time": 0.0}
    for entry in parser:
        if not entry:
            continue
        url = entry["url"]
        request_time = float(entry["request_time"])
        if url not in entries:
            entries[url] = []
        entries[url].append(request_time)
        total["entries"] += 1
        total["request_time"] += request_time
    log.info(message="Finished log entries parsing")
    return ParserOutput(entries, total)


def calculate_metrics(etnries: Dict, total: Dict) -> List[Dict]:
    log = structlog.get_logger()
    log.info(message="Calculating metrics", total=total)
    metrics: List[Dict] = []
    for url in etnries:
        entry_metrics: Dict = {
            "url": url,
            "count": len(etnries[url]),
            "count_perc": len(etnries[url]) / total["entries"] * 100,
            "time_sum": sum(etnries[url]),
            "time_perc": sum(etnries[url]) / total["request_time"] * 100,
            "time_avg": statistics.mean(etnries[url]),
            "time_max": max(etnries[url]),
            "time_med": statistics.median(etnries[url]),
        }
        metrics.append(entry_metrics)
    return metrics


def report_dir_exists(report_dir: Optional[str]):
    log = structlog.get_logger()
    log.info(message="Checking report dir exists", report_dir=report_dir)
    if not report_dir:
        log.info(
            message="Report dir path empty or None, returned False",
            report_dir=report_dir,
        )
        return False
    return os.path.exists(report_dir)


def sort_metrics(metrics: List[Dict]) -> List[Dict]:
    log = structlog.get_logger()
    log.info(message='Sorting metrics by "time_sum"')
    metrics.sort(reverse=True, key=lambda d: (d.get("time_sum", 0.0), 0))
    log.info(message="Metrics sorted")
    return metrics


def truncate_metrics(metrics: List[Dict], size: int) -> List[Dict]:
    log = structlog.get_logger()
    log.info(message="Truncating metrics", current_size=len(metrics), truncate_size=size)
    return metrics[0:size]


def get_json_metrics(metrics: List[Dict]) -> str:
    log = structlog.get_logger()
    log.info(message="Converting metrics to json")
    return json.dumps(metrics)


def get_report_template(template_path: str) -> Optional[str]:
    log = structlog.get_logger()
    log.info(message="Getting report template content")
    try:
        return open(template_path, encoding="utf-8").read()
    except FileNotFoundError:
        log.error(
            message="Report template file not found",
            template_path=template_path,
        )
        return None
    except UnicodeDecodeError:
        log.error(
            message="Can not decode report template",
            template_path=template_path,
        )
        return None


def insert_report_content(template: str, json_metrics: str) -> str:
    log = structlog.get_logger()
    log.info(message="Inserting metrics into template")
    return template.replace("$table_json", json_metrics)


def get_report_path(report_dir: Optional[str], report_date: str) -> Optional[str]:
    log = structlog.get_logger()
    log.info(message="Getting report path", report_dir=report_dir, report_date=report_date)
    if not report_dir:
        log.info(message='Report dir path empty or None, returned ""', report_dir=report_dir, )
        return None
    return f"{report_dir}/report-{report_date}.html"


def get_config_path(argv: List[str]) -> Optional[str]:
    try:
        return argv[argv.index("--config") + 1]
    except IndexError:
        return None


def read_config(config_path: str) -> Optional[str]:
    if not config_path:
        return None
    try:
        return open(config_path, encoding="utf-8").read()
    except FileNotFoundError:
        return None


def load_config(config_text: str) -> Dict:
    if not config_text:
        return {}
    try:
        return json.loads(config_text)
    except json.JSONDecodeError:
        return {}


def apply_config(app_config: Dict, ext_config: Dict) -> Dict:
    if not app_config:
        return {}
    if not ext_config:
        return {}
    app_config.update(ext_config)
    return app_config


def set_config(argv: List[str]) -> bool:
    return "--config" in argv


def handle_exception(ex_type, _, traceback) -> None:
    formated_traceback = ""
    while traceback:
        filename = traceback.tb_frame.f_code.co_filename
        name = traceback.tb_frame.f_code.co_name
        line_no = traceback.tb_lineno
        formated_traceback += f"\nFile {filename} line {line_no}, in {name}"
        traceback = traceback.tb_next
    log = structlog.get_logger()
    log.error(message="Unexpected error", error=ex_type.__name__, traceback=formated_traceback)


def conf_logger(app_log_file: Optional[str]) -> None:
    handlers: List[logging.Handler] = [logging.StreamHandler()]
    if app_log_file:
        handlers.append(logging.FileHandler(app_log_file))
    logging.basicConfig(level=logging.DEBUG, format="%(message)s", handlers=handlers)
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )
    sys.excepthook = handle_exception


def save_report(report_path: str, report: str) -> int:
    log = structlog.get_logger()
    log.info(message="Saving file", report_path=report_path)
    return open(report_path, mode="w", encoding="utf-8").write(report)


def main(argv: List[str]) -> None:
    app_config = config.copy()
    if set_config(argv):
        ext_config_path = get_config_path(argv)
        if not ext_config_path:
            sys.exit()
        ext_config_text = read_config(ext_config_path)
        if not ext_config_text:
            sys.exit()
        ext_config = load_config(ext_config_text)
        if not ext_config:
            sys.exit()
        app_config = apply_config(app_config, ext_config)
        if not app_config:
            sys.exit()
    conf_logger(app_config.get("LOG_FILE"))
    log = structlog.get_logger()
    log.info(message="Application started", app_config=app_config)
    log_dir = app_config.get("LOG_DIR")
    report_dir = app_config.get("REPORT_DIR")
    report_size = int(str(app_config.get("REPORT_SIZE")))
    if not log_dir_exists(log_dir):
        log.error(message="Application exited: log dir does not exists")
        exit()
    log_files = get_log_files(log_dir)
    if not log_files:
        log.error(message="Application exited: log dir is empty")
        exit()
    latest_log = search_latest(log_files)
    if not latest_log or not latest_log.name:
        log.error(message="Application exited: lates log could not be found")
        exit()
    report_path = get_report_path(report_dir, latest_log.date)
    if not report_path:
        log.error(message="Application exited: failed to get report path")
        exit()
    if os.path.exists(report_path):
        log.info(
            message="Application exited: report for latest log already exists",
            latest_log=latest_log.name,
            report_path=report_path,
        )
        exit()
    log_path = get_log_path(latest_log.name, log_dir)
    if not log_path:
        log.error(message="Application exited: failed to get log path")
        exit()
    try:
        log_file: IO[str] = (
            gzip.open(str(log_path), mode="rt", encoding="utf-8")
            if latest_log.extention == ".gz"
            else open(str(log_path), encoding="utf-8")
        )
    except FileNotFoundError:
        log.error(
            message="Application exited: latest log file could not be found",
            log_path=log_path,
            log_extention=latest_log.extention,
        )
        exit()
    except gzip.BadGzipFile:
        log.error(
            message="Application exited: invalid gzip file",
            log_path=log_path,
            log_extention=latest_log.extention,
        )
        exit()
    parser = entries_parser(log_file)
    parser_output = parse_entries(parser)
    metrics = calculate_metrics(parser_output.entries, parser_output.total)
    metrics = sort_metrics(metrics)
    metrics = truncate_metrics(metrics, report_size)
    metrics_json = get_json_metrics(metrics)
    if not report_dir_exists(report_dir):
        log.error(message="Application exited: report dir does not exists")
        exit()
    report_template = get_report_template("report.html")
    if not report_template:
        log.error(message="Application exited: failed to get report template")
        exit()
    report = insert_report_content(str(report_template), metrics_json)
    save_report(report_path, report)