-----

# 🌌 ASTRO-NET v2.5：全时空运力数字孪生平台

**ASTRO-NET** 是一个城市级低空与地面物流运力对比推演的 **3D 数字孪生可视化平台**。本项目以深圳市南山区真实脱敏运单数据为基座，通过多线程路网解算、空间聚类挖掘以及 WebGL 高性能渲染，直观、量化地展示了\*\*“无人机低空直达”**相对于**“传统地面外卖骑手”\*\*在复杂城市地形下的降维打击优势。

-----

## ✨ 核心亮点 (Key Features)

### 1\. 🚀 双轨时空推演引擎

  * **地面真实路网寻路**：基于本地私有化部署的 **OSRM (Open Source Routing Machine)** 引擎，精准还原骑手在真实物理街道中的绕行、红绿灯耗时与路径轨迹。
  * **低空仿生避障航线**：自动生成包含“垂直起降”、“高度爬升”与“法向量横向绕飞”的 3D 无人机航线，完美越过城市超高层建筑白模。
  * **异常数据清洗**：自动识别并剔除 OSRM 寻路失败导致的“穿墙直线”脏数据，保证孪生场景的严谨性。

### 2\. 🎮 沉浸式 FPV 同订单对比 (Split-Screen Chase Camera)

  * **双端 WebGL 独立渲染**：在屏幕上同时开启左右两个完全独立的 `Deck.gl` 实例，彻底解决上下文冲突与图层重叠问题。
  * **时空同步等待逻辑**：强制锁定同一订单。无人机凭借空间优势提前抵达终点后，会悬停保持最后姿态**原地等待**，直到左侧地面骑手历经拥堵到达终点，系统才会平滑切入下一活跃订单，极具商业说服力。
  * **动态位姿追踪 (Bearing Tracking)**：实时计算实体移动矢量（$\vec{v}$），驱动相机镜头随轨迹转弯平滑旋转，营造赛车游戏般的第三人称追尾体验。

### 3\. 📊 宏观战略可视化图层

  * **🔥 供需热力网格 (Hexagon Layer)**：利用 **DBSCAN** 聚类算法识别运力过载的“痛点商圈”，以 3D 赛博金字塔形式展现。
  * **☂️ OD 流量干线 (Arc Layer)**：将海量散点订单收敛为城市物流主干网，提取高频路线，以空中抛物飞线展示低空枢纽布局。
  * **🏙️ 动态城市白模 (Polygon Layer)**：基于 **OSMnx** 动态抓取开源地图中的真实建筑轮廓与高度数据，构建 3D 避障基准。

### 4\. 🎛️ 专业级调度指挥舱 UI

  * 支持 **1x/5x/10x/20x** 任意切换的全局时间轴流速控制。
  * **全局 / 宏观 / 微观** 视角的丝滑飞行过渡（`FlyToInterpolator`）。
  * 左侧面板实时统计：在途运力、时间节省（小时）、运力饱和度及平均速度等核心商业指标。

-----

## 🛠️ 技术栈 (Tech Stack)

### 前端 (Frontend)

  * **核心渲染**：`Deck.gl (v8.9.0)` （由 Uber 开源的大规模数据 WebGL 可视化框架）
  * **底层地图**：`MapLibre GL JS` (接入 Carto Dark Matter 极简暗色底图)
  * **UI/交互**：原生 HTML5 / CSS3 / Vanilla JavaScript

### 后端与数据处理 (Backend & Data Pipeline)

  * **语言**：`Python 3.x`
  * **并发处理**：`concurrent.futures.ThreadPoolExecutor` (多线程加速寻路)
  * **空间数据与路网**：
      * `OSRM (Docker)`: 本地路网寻路服务器。
      * `OSMnx`: 抓取 OSM 真实建筑轮廓。
  * **数据挖掘**：`scikit-learn` (DBSCAN 聚类), `Pandas`, `Numpy`

-----

## 📂 项目结构 (Project Structure)

```text
ASTRO-NET/
│
├── main.py                     # Python 核心数据引擎 (路径生成, 聚类, 抓取建筑)
├── index.html                  # 前端 3D 数字孪生可视化主面板 (Deck.gl)
├── 深圳市南山区运单抽样_已脱敏.csv # 原始输入的测试订单数据集
│
├── project_data.json           # [自动生成] 核心输出: 包含3D建筑、骑手/无人机逐帧轨迹
├── od_matrix.json              # [自动生成] 输出: OD 拓扑干线矩阵
├── hotspots.json               # [自动生成] 输出: DBSCAN 聚类生成的热点区域
│
└── .gitignore                  # Git 忽略配置 (忽略 .idea, __pycache__, 大数据文件等)
```

-----

## ⚙️ 快速启动 (Quick Start)

### 1\. 启动 OSRM 路网引擎 (前置依赖)

Python 脚本在生成地面路径时，高度依赖本地的 OSRM 服务。请确保已通过 Docker 启动了相应区域（如广东/深圳）的路网服务并映射至 `5000` 端口：

```bash
# 示例启动命令 (需提前准备好 .osrm 编译数据)
docker run -t -i -p 5000:5000 -v "${PWD}:/data" osrm/osrm-backend osrm-routed --algorithm mld /data/guangdong-latest.osrm
```

### 2\. 运行 Python 数据管道

安装依赖并执行 `main.py` 进行多线程数据推演：

```bash
pip install pandas numpy requests tqdm osmnx scikit-learn

python main.py
```

> *注：脚本运行完毕后，根目录下会自动生成 `project_data.json`, `od_matrix.json`, `hotspots.json` 三个文件。*

### 3\. 启动前端可视化服务

由于浏览器的 CORS（跨域）安全限制，不能直接双击打开 `.html`。请在项目根目录启动一个本地 Web 服务：

```bash
# 使用 Python 自带的简易 HTTP 服务器
python -m http.server 8000
```

随后在浏览器中访问：`http://localhost:8000/index.html` 即可进入指挥舱。

-----

## 🎮 操作指南 (User Guide)

1.  **总览态势**：进入页面后，点击右侧的 `🔥 时空热力图` 和 `☂️ 3D OD流量图`，配合时间轴播放，向观众展示当前城市的地面运力痛点及你们规划的低空网络。
2.  **微观避障**：打开 `☄️ 3D建筑模型` 和 `运力流光`，点击 `🚁 避障细节` 视角，展示无人机的 3D 爬升与绕飞算法。
3.  **同订单降维打击 (Killer Feature)**：
      * 确保动画处于播放状态，点击右侧 `👁️ 同订单对比`。
      * 屏幕一分为二，左侧跟踪被路网困住的骑手，右侧跟踪低空直达的无人机。
      * **观察细节**：当无人机极速到达后，画面会悬停等待，右上方提示变为“已极速送达，原地等待骑手...”，视觉冲击力极强。

-----

## 🤝 开发者 (Developer)

**[你的名字 / 团队名字]** *架构设计 / 算法推演 / 全栈开发*