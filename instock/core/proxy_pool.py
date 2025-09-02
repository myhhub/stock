import os
import json
import datetime
import requests
import random
from pathlib import Path

# 如果下面目标站不可用，请使用test.ipw.cn、ip.sb、ipinfo.io、ip-api.com、64.ipcheck.ing
def get_proxy(proxy=None, pool_size=3):
    """
    基于青果代理获取动态代理IP，带缓存机制
    
    Args:
        proxy: 可选的代理配置
        pool_size: 连接池大小，默认为3
    
    Returns:
        代理配置字典或None
    """
    
    # 缓存文件路径
    cache_file = Path(__file__).parent / f"proxy_cache_pool_{pool_size}.json"
    
    # 检查缓存文件是否存在且有效
    if cache_file.exists():
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)
                
            # 检查代理池中的代理是否还有效
            proxy_pool = cached_data.get('proxy_pool', [])
            if proxy_pool:
                now = datetime.datetime.now()
                # 检查代理池中是否有未过期的代理
                valid_proxies = []
                for proxy_config in proxy_pool:
                    deadline_str = proxy_config.get('deadline', '')
                    if deadline_str:
                        try:
                            deadline = datetime.datetime.strptime(deadline_str, "%Y-%m-%d %H:%M:%S")
                            # 如果代理还有15秒以上才过期，认为有效
                            if deadline > now + datetime.timedelta(seconds=15):
                                valid_proxies.append(proxy_config)
                        except ValueError:
                            continue
                
                # 如果有有效代理，随机返回一个
                if valid_proxies:
                    random_proxy = random.choice(valid_proxies)
                    print(f"使用缓存中的代理:{random_proxy}")
                    return {
                        "http": random_proxy["http"],
                        "https": random_proxy["https"]
                    }
                else:
                    print("缓存中的代理已过期，重新获取...")
        except (json.JSONDecodeError, KeyError, ValueError):
            print("缓存文件损坏，重新获取代理...")
    
    # 从环境变量获取认证信息
    authKey = os.getenv("qg_authKey")
    password = os.getenv("qg_password")
    
    if not authKey or not password:
        print("环境变量 qg_authKey 或 qg_password 未设置")
        return None
    
    qingguo_url = f"https://share.proxy.qg.net/get?key={authKey}&num={pool_size}"
    
    try:
        resp = requests.get(qingguo_url, proxies=proxy, timeout=10)
        
        if resp.status_code == 200:
            output = resp.json()
            data = output.get("data", [])
            if len(data) == 0:
                print("API返回空数据")
                return None
            
            # 构建代理池
            proxy_pool = []
            for proxy_data in data:
                proxyUrl = "http://%(user)s:%(password)s@%(server)s" % {
                    "user": authKey,
                    "password": password,
                    "server": proxy_data.get("server", ""),
                }
                
                proxy_config = {
                    "http": proxyUrl,
                    "https": proxyUrl,
                    "proxy_ip": proxy_data.get("proxy_ip", ""),
                    "server": proxy_data.get("server", ""),
                    "area": proxy_data.get("area", ""),
                    "isp": proxy_data.get("isp", ""),
                    "deadline": proxy_data.get("deadline", "")
                }
                proxy_pool.append(proxy_config)
            
            # 保存到缓存文件
            cache_data = {
                "proxy_pool": proxy_pool,
                "pool_size": pool_size,
                "timestamp": datetime.datetime.now().isoformat(),
                "total_proxies": len(proxy_pool)
            }
            
            # 确保目录存在并保存缓存
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            # 随机返回一个代理
            random_proxy = random.choice(proxy_pool)
            print(f"获取到 {len(proxy_pool)} 个新代理，随机选择: {random_proxy['proxy_ip']} ({random_proxy.get('area', '')})")
            
            return {
                "http": random_proxy["http"],
                "https": random_proxy["https"]
            }
        else:
            print(f"获取代理失败，状态码: {resp.status_code}")
            
    except requests.RequestException as e:
        print(f"网络请求失败: {e}")
        # 网络异常时，尝试使用缓存中未过期的代理
        if cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)
                proxy_pool = cached_data.get('proxy_pool', [])
                if proxy_pool:
                    # 即使网络异常，也只使用未过期的代理
                    now = datetime.datetime.now()
                    valid_proxies = []
                    for proxy_config in proxy_pool:
                        deadline_str = proxy_config.get('deadline', '')
                        if deadline_str:
                            try:
                                deadline = datetime.datetime.strptime(deadline_str, "%Y-%m-%d %H:%M:%S")
                                if deadline > now:
                                    valid_proxies.append(proxy_config)
                            except ValueError:
                                continue
                    
                    if valid_proxies:
                        random_proxy = random.choice(valid_proxies)
                        print(f"网络异常，使用缓存代理: {random_proxy['proxy_ip']}")
                        return {
                            "http": random_proxy["http"],
                            "https": random_proxy["https"]
                        }
            except:
                pass
    
    return None


if __name__ == "__main__":
    # 测试代理获取
    print("=== 测试获取代理 ===")
    proxy = get_proxy(pool_size=3)
    print("获取到的代理:", proxy)
    
    # 多次调用测试随机性
    print("\n=== 测试随机性 ===")
    for i in range(3):
        proxy = get_proxy(pool_size=3)
        if proxy:
            print(f"第 {i+1} 次调用获取的代理:", proxy)
    
    # 测试网络连接
    if proxy:
        try:
            response = requests.get("http://ip.sb", proxies=proxy, timeout=10)
            print(response.status_code)
        except Exception as e:
            print(f"代理测试失败: {e}")
