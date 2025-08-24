/**
 * Luckysheet适配器 - 模拟SpreadJS的API
 * 让原有的股票系统代码可以无缝使用Luckysheet
 */

// 数据管理器适配器
class LucksheetDataManagerAdapter {
    constructor() {
        this.tables = {};
    }
    
    addTable(name, config) {
        const table = new LucksheetTableAdapter(name, config);
        this.tables[name] = table;
        return table;
    }
}

// 数据表适配器
class LucksheetTableAdapter {
    constructor(name, config) {
        this.name = name;
        this.config = config;
        this.views = {};
    }
    
    addView(name, columns) {
        const view = new LucksheetTableViewAdapter(name, columns, this.config);
        this.views[name] = view;
        return view;
    }
}

// 数据视图适配器
class LucksheetTableViewAdapter {
    constructor(name, columns, config) {
        this.name = name;
        this.columns = columns;
        this.config = config;
        this.data = [];
        this.filteredData = [];
    }
    
    async fetch() {
        if (this.config && this.config.remote && this.config.remote.read) {
            try {
                const response = await fetch(this.config.remote.read.url);
                const data = await response.json();
                this.data = data;
                this.filteredData = data.slice();
                return data;
            } catch (error) {
                console.error('Error fetching data:', error);
                return [];
            }
        }
        return [];
    }
    
    length() {
        return this.data.length;
    }
    
    visibleLength() {
        return this.filteredData.length;
    }
    
    clearFilter() {
        this.filteredData = this.data.slice();
    }
}

// 工作表适配器
class LucksheetSheetAdapter {
    constructor(workbook, name, index) {
        this.workbook = workbook;
        this.name = name;
        this.index = index;
        this.dataView = null;
        this.pinnedColumns = [];
        this.options = {};
    }
    
    setDataView(dataView) {
        this.dataView = dataView;
        this.updateDisplay();
    }
    
    updateDisplay() {
        if (!this.dataView || !this.dataView.data) return;
        
        const container = this.workbook.container;
        const tableContainer = container.querySelector(`#${container.id}_table`);
        
        if (tableContainer) {
            const headerElement = container.querySelector(`#${container.id}_header`);
            const bodyElement = container.querySelector(`#${container.id}_body`);
            
            // 生成表头
            if (this.dataView.columns && headerElement) {
                headerElement.innerHTML = '<tr>' + 
                    this.dataView.columns.map(col => `<th>${col.displayText || col.field}</th>`).join('') + 
                    '</tr>';
            }
            
            // 生成数据行
            if (bodyElement && this.dataView.filteredData) {
                bodyElement.innerHTML = this.dataView.filteredData.map((row, rowIndex) => {
                    const cells = this.dataView.columns.map((col, colIndex) => {
                        let value = row[col.field] || '';
                        
                        // 如果是代码列且有超链接样式
                        if (col.field === 'code' && col.style && col.style.cellType && col.style.cellType.clickAction) {
                            value = `<a href="#" onclick="handleCellClick(${rowIndex}, ${colIndex}); return false;" style="color: blue; text-decoration: underline;">${value}</a>`;
                        }
                        
                        return `<td>${value}</td>`;
                    }).join('');
                    return `<tr>${cells}</tr>`;
                }).join('');
                
                // 绑定点击事件
                window.handleCellClick = (row, col) => {
                    if (this.dataView.columns[col].style && this.dataView.columns[col].style.cellType && this.dataView.columns[col].style.cellType.clickAction) {
                        this.dataView.columns[col].style.cellType.clickAction({row: row});
                    }
                };
            }
        }
        
        // 触发选择变化事件
        this.workbook.trigger('selectionChanged', {
            newSelections: [{row: 0, rowCount: 1}]
        });
    }
    
    togglePinnedColumns(columns) {
        this.pinnedColumns = columns;
    }
    
    applyTableTheme(theme) {
        // Luckysheet有自己的主题系统
    }
    
    setDefaultRowHeight(height, area) {
        // Luckysheet会自动处理行高
    }
    
    getSheet() {
        return {
            getValue: (row, col) => {
                if (this.dataView && this.dataView.filteredData[row] && this.dataView.columns[col]) {
                    return this.dataView.filteredData[row][this.dataView.columns[col].field];
                }
                return null;
            },
            rowFilter: () => ({
                reset: () => {
                    if (this.dataView) {
                        this.dataView.clearFilter();
                        this.workbook.refresh();
                    }
                }
            })
        };
    }
}

// SpreadJS样式和事件适配器
const GCSpreadEvents = {
    SelectionChanged: 'selectionChanged',
    RangeFiltered: 'rangeFiltered',
    RangeFilterCleared: 'rangeFilterCleared'
};

// StatusBar适配器
const GCSpreadStatusBar = {
    StatusItem: class {
        constructor(name, options = {}) {
            this.name = name;
            this.options = options;
            this._element = null;
        }
        
        onCreateItemView(container) {
            // 子类实现
        }
        
        onBind(context) {
            // 子类实现
        }
        
        onUpdate() {
            // 子类实现
        }
    },
    
    StatusBar: class {
        constructor(container, options = {}) {
            this.container = container;
            this.options = options;
            this.items = {};
            this.boundContext = null;
            
            if (options.items) {
                options.items.forEach(item => {
                    this.items[item.name] = item;
                    item.onCreateItemView(container);
                });
            }
        }
        
        bind(context) {
            this.boundContext = context;
            Object.values(this.items).forEach(item => {
                if (item.onBind) {
                    item.onBind(context);
                }
            });
        }
        
        get(itemName) {
            return this.items[itemName];
        }
    }
};

// SpreadJS Excel适配器
const GCSpreadExcelIO = class {
    save(data, successCallback, errorCallback) {
        try {
            // 使用Luckysheet的导出功能
            const container = document.querySelector('[data-luckysheet]');
            if (container && window.luckysheet) {
                luckysheet.exportXlsx({
                    title: 'export',
                    success: function(blob) {
                        if (successCallback) successCallback(blob);
                    },
                    error: function(error) {
                        if (errorCallback) errorCallback(error);
                    }
                });
            } else {
                // 备用方案：创建简单的Excel数据
                const workbook = { sheets: [] };
                if (successCallback) {
                    const blob = new Blob([JSON.stringify(workbook)], {type: 'application/json'});
                    successCallback(blob);
                }
            }
        } catch (error) {
            if (errorCallback) errorCallback(error);
        }
    }
};

// 主要的GC.Spread.Sheets命名空间
window.GC = window.GC || {};
window.GC.Spread = window.GC.Spread || {};
window.GC.Spread.Sheets = {
    // 工作簿类
    Workbook: class {
        constructor(container, options = {}) {
            if (typeof container === 'string') {
                container = document.getElementById(container);
            }
            
            this.container = container;
            this.options = options;
            this.sheets = [];
            this.activeSheet = null;
            this._dataManager = new LucksheetDataManagerAdapter();
            this.eventHandlers = {};
            
            // 初始化Luckysheet
            this.initLuckysheet();
        }
        
        initLuckysheet() {
            if (typeof luckysheet !== 'undefined') {
                // 如果Luckysheet可用，初始化它
                const options = {
                    container: this.container.id,
                    title: 'Stock Data',
                    lang: 'zh',
                    data: []
                };
                
                luckysheet.create(options);
                this.container.setAttribute('data-luckysheet', 'true');
            } else {
                // 如果Luckysheet不可用，创建备用表格
                this.createFallbackTable();
            }
        }
        
        createFallbackTable() {
            this.container.innerHTML = `
                <div style="width: 100%; height: 100%; border: 1px solid #ccc; background: white;">
                    <div id="${this.container.id}_table" style="width: 100%; height: 100%; overflow: auto;">
                        <table class="table table-striped table-bordered" style="margin: 0;">
                            <thead id="${this.container.id}_header"></thead>
                            <tbody id="${this.container.id}_body"></tbody>
                        </table>
                    </div>
                </div>
            `;
        }
        
        addSheetTab(index, name, sheetType) {
            const sheet = new LucksheetSheetAdapter(this, name, index);
            this.sheets[index] = sheet;
            this.activeSheet = sheet;
            return sheet;
        }
        
        getActiveSheetTab() {
            return this.activeSheet;
        }
        
        dataManager() {
            return this._dataManager;
        }
        
        suspendPaint() {
            // Luckysheet不需要这个操作
        }
        
        resumePaint() {
            // Luckysheet不需要这个操作
        }
        
        toJSON(options = {}) {
            return {
                sheets: this.sheets.map(sheet => ({
                    name: sheet.name,
                    data: sheet.dataView ? sheet.dataView.data : []
                }))
            };
        }
        
        refresh() {
            if (this.activeSheet && this.activeSheet.dataView) {
                this.activeSheet.updateDisplay();
            }
        }
        
        bind(eventName, handler) {
            if (!this.eventHandlers[eventName]) {
                this.eventHandlers[eventName] = [];
            }
            this.eventHandlers[eventName].push(handler);
        }
        
        trigger(eventName, args) {
            if (this.eventHandlers[eventName]) {
                this.eventHandlers[eventName].forEach(handler => {
                    handler(null, args);
                });
            }
        }
    },
    
    // 工作表类型枚举
    SheetType: {
        tableSheet: 'tableSheet'
    },
    
    // 工作表区域枚举
    SheetArea: {
        colHeader: 'colHeader'
    },
    
    // 事件枚举
    Events: GCSpreadEvents,
    
    // 状态栏
    StatusBar: GCSpreadStatusBar,
    
    // 表格主题
    Tables: {
        TableThemes: {
            light18: 'light18'
        }
    },
    
    // 样式类
    Style: class {
        constructor() {
            this.cellType = null;
        }
    },
    
    // 单元格类型
    CellTypes: {
        HyperLink: class {
            constructor() {
                this.clickAction = null;
            }
            
            onClickAction(callback) {
                this.clickAction = callback;
            }
        }
    },
    
    // Excel IO
    Excel: {
        IO: GCSpreadExcelIO
    },
    
    // 查找控件
    findControl: function(element) {
        // 返回与元素关联的工作簿实例
        if (element.luckysheetInstance) {
            return element.luckysheetInstance;
        }
        return null;
    }
};
