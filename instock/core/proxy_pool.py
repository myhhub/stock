import os
import json
import datetime
import requests
from pathlib import Path

# 如果下面目标站不可用，请使用test.ipw.cn、ip.sb、ipinfo.io、ip-api.com、64.ipcheck.ing
def get_proxy(proxy=None):
    """
    基于青果代理获取动态代理IP，带缓存机制
    {
    "code": "SUCCESS",
    "data": [
        {
        "proxy_ip": "42.49.58.5",
        "server": "60.188.79.112:20040",
        "area_code": 431300,
        "area": "湖南省娄底市",
        "isp": "联通",
        "deadline": "2025-08-24 18:14:40"
        }
    ],
    "request_id": "b24f8a18-33f4-4702-9de3-2fd9c26cd5c4"
    }
    """
    
    # 缓存文件路径
    cache_file = Path(__file__).parent / "proxy_cache.json"
    
    # 检查缓存文件是否存在
    if cache_file.exists():
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)
                
            # 检查是否有有效的代理
            if 'deadline' in cached_data and cached_data['deadline']:
                deadline_str = cached_data['deadline']
                deadline = datetime.datetime.strptime(deadline_str, "%Y-%m-%d %H:%M:%S")
                now = datetime.datetime.now()
                print(f"当前时间: {now}, 代理过期时间: {deadline}, 剩余时间: {deadline - now}")
                # 如果代理还有15秒以上才过期，返回缓存的代理
                if deadline > now + datetime.timedelta(seconds=15):
                    print( f"使用缓存的代理! 过期时间: {deadline_str}" )
                    return cached_data.get('proxy')
        except (json.JSONDecodeError, KeyError, ValueError):
            # 如果缓存文件损坏，忽略并重新获取
            pass
    
    # 从环境变量获取认证信息
    authKey = os.getenv("qg_authKey")
    password = os.getenv("qg_password")
    
    if not authKey or not password:
        return None
    
    qingguo_url = f"https://share.proxy.qg.net/get?key={authKey}"
    
    try:
        resp = requests.get(qingguo_url, proxies=proxy, timeout=10)
        
        if resp.status_code == 200:
            output = resp.json()
            data = output.get("data", [])
            if len(data) == 0:
                return None
            
            data = data[0]
            proxyUrl = "http://%(user)s:%(password)s@%(server)s" % {
                "user": authKey,
                "password": password,
                "server": data.get("server", ""),
            }
            
            proxy_config = {
                "http": proxyUrl,
                "https": proxyUrl
            }
            
            # 保存到缓存文件
            cache_data = {
                "proxy": proxy_config,
                "deadline": data.get("deadline", ""),
                "server": data.get("server", ""),
                "proxy_ip": data.get("proxy_ip", ""),
                "timestamp": datetime.datetime.now().isoformat()
            }
            
            # 确保目录存在
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            print( f"获取到新的代理! 过期时间: {cache_data['deadline']}" )
            with open(cache_file, 'w', encoding='utf-8') as f:
                print( f"缓存代理信息: {json.dumps(cache_data, ensure_ascii=False, indent=2)}" )
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            return proxy_config
        else:
            print(f"获取代理失败，状态码: {resp.status_code}")
            
    except requests.RequestException:
        # 网络请求失败，尝试使用缓存
        if cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)
                return cached_data.get('proxy')
            except:
                pass
    
    return None


def clear_proxy_cache():
    """清除代理缓存"""
    cache_file = Path(__file__).parent / "proxy_cache.json"
    if cache_file.exists():
        cache_file.unlink()


def get_proxy_info():
    """获取当前缓存的代理信息"""
    cache_file = Path(__file__).parent / "proxy_cache.json"
    
    if cache_file.exists():
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    
    return None


if __name__ == "__main__":
    # 测试代理获取
    proxy = get_proxy()
    print("获取到的代理:", proxy)
    
    # 查看缓存信息
    info = get_proxy_info()
    if info:
        print("缓存信息:", json.dumps(info, ensure_ascii=False, indent=2))
    requests = requests.get("http://ip.sb", proxies=proxy, timeout=10)
    print("使用代理访问 http://ip.sb 的结果:", requests.text)
