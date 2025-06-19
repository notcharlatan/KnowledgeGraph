import pandas as pd
import os
from datetime import datetime

def calculate_rating(distance, weather_score, method='balanced'):
    """
    根据航线距离和天气评分计算综合评分
    
    参数:
    distance: 航线距离(海里)
    weather_score: 天气影响评分(1-10)
    method: 评分计算方法，可选'balanced'(平衡)、'distance_weighted'(距离优先)、'weather_weighted'(天气优先)
    
    返回:
    综合评分(0-100)
    """
    # 标准化距离到0-100范围（假设最大距离为20000海里）
    max_distance = 20000
    distance_score = min(100, (distance / max_distance) * 100)
    
    # 转换天气评分为0-100范围（直接乘以10）
    weather_score_scaled = weather_score * 10
    
    if method == 'balanced':
        # 平衡距离和天气的影响（各占50%）
        return round(0.5 * distance_score + 0.5 * weather_score_scaled, 1)
    elif method == 'distance_weighted':
        # 距离影响占70%，天气占30%
        return round(0.7 * distance_score + 0.3 * weather_score_scaled, 1)
    elif method == 'weather_weighted':
        # 天气影响占70%，距离占30%
        return round(0.3 * distance_score + 0.7 * weather_score_scaled, 1)
    else:
        raise ValueError(f"未知的评分方法: {method}")

def add_rating_column(csv_file, output_file=None, rating_method='balanced'):
    """
    为CSV文件添加评分列
    
    参数:
    csv_file: 输入CSV文件路径
    output_file: 输出CSV文件路径，默认为原文件添加_rated后缀
    rating_method: 评分计算方法，见calculate_rating函数说明
    """
    try:
        # 读取CSV文件
        df = pd.read_csv(csv_file)
        
        # 检查必要列是否存在
        if '航线距离(海里)' not in df.columns or '航线天气影响评分(1-10)' not in df.columns:
            raise ValueError("CSV文件中缺少'航线距离(海里)'或'航线天气影响评分(1-10)'列")
        
        # 计算评分
        df['评分'] = df.apply(
            lambda row: calculate_rating(
                row['航线距离(海里)'], 
                row['航线天气影响评分(1-10)'],
                method=rating_method
            ),
            axis=1
        )
        
        # 设置输出文件路径
        if output_file is None:
            file_name, file_ext = os.path.splitext(csv_file)
            output_file = f"{file_name}_rated{file_ext}"
        
        # 保存结果
        df.to_csv(output_file, index=False, encoding='utf-8-sig')  # 使用utf-8-sig确保中文CSV正确显示
        
        print(f"成功添加评分列，保存到: {output_file}")
        print(f"评分方法: {rating_method}")
        print(f"评分范围: {df['评分'].min()} - {df['评分'].max()}，平均值: {df['评分'].mean():.2f}")
        
        return output_file
    
    except FileNotFoundError:
        print(f"错误: 找不到文件 {csv_file}")
        return None
    except Exception as e:
        print(f"处理文件时出错: {str(e)}")
        return None

if __name__ == "__main__":
    # 设置文件路径（请根据实际情况修改）
    input_file = r"PythonProject\data\全球航线数据.CSV"
    
    # 可选的评分方法: 'balanced', 'distance_weighted', 'weather_weighted'
    rating_method = 'balanced'  # 平衡距离和天气的影响
    
    # 执行添加评分列操作
    add_rating_column(input_file, rating_method=rating_method)
    
    # 示例：使用不同的评分方法
    # add_rating_column(input_file, rating_method='distance_weighted')
    # add_rating_column(input_file, rating_method='weather_weighted')