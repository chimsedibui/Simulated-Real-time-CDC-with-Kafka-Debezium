"""Role-based marketplace dashboards, using completed mart snapshots only.

No gold/source queries, external panel plugins, or warehouse schema changes.
"""
from copy import deepcopy

CH = {'type': 'grafana-clickhouse-datasource', 'uid': 'clickhouse'}
SNAPSHOT = 'refresh_id = ${refresh_id:raw}'
ANCHOR = f'(SELECT max(order_date) FROM mart.daily_sales WHERE {SNAPSHOT})'
PERIOD = f'order_date >= {ANCHOR} - INTERVAL ${{period_days:raw}} DAY AND order_date < {ANCHOR}'
PREVIOUS = f'order_date >= {ANCHOR} - INTERVAL (${{period_days:raw}} * 2) DAY AND order_date < {ANCHOR} - INTERVAL ${{period_days:raw}} DAY'
ALL_SCOPE = 'Lũy kế tại bản dữ liệu đang chọn; không lọc theo ngày.'
COLORS = {'blue': '#5794F2', 'green': '#73BF69', 'amber': '#FFB357', 'red': '#F2495C', 'purple': '#B877D9', 'cyan': '#56D2CB'}
NAV = [
    ('cdc-business', '01 · Điều hành'),
    ('cdc-operations', '02 · Đơn hàng & trải nghiệm'),
    ('cdc-growth', '03 · Danh mục & chiến dịch'),
    ('cdc-sellers', '04 · Nhà bán & tồn kho'),
    ('cdc-finance', '05 · Thu phí & dòng tiền'),
]


def sql(query):
    return {'refId': 'A', 'editorType': 'sql', 'rawSql': query, 'format': 1,
            'pluginVersion': '4.11.2', 'queryType': 'table'}


def query_variable(name, label, query, multi=False, hidden=False):
    result = dict(name=name, label=label, type='query', datasource=deepcopy(CH), query=query,
                  refresh=2, hide=2 if hidden else 0, multi=multi, includeAll=multi,
                  options=[], current={}, sort=1 if multi else 0, skipUrlSync=hidden)
    if multi:
        result['current'] = {'text': 'All', 'value': '$__all'}
    return result


def custom_variable(name, label, values, default):
    return dict(name=name, label=label, type='custom', query=','.join(map(str, values)),
                current={'text': str(default), 'value': str(default)},
                options=[{'text': str(x), 'value': str(x), 'selected': x == default} for x in values],
                multi=False, includeAll=False, hide=0)


class Board:
    def __init__(self, uid, title, purpose):
        self.y = 0
        self.doc = dict(uid=uid, title=title, description=purpose, schemaVersion=39, version=1,
                        tags=['cdc', 'marketplace', 'business'], timezone='Asia/Ho_Chi_Minh',
                        refresh='1m', editable=True, time={'from': 'now-1h', 'to': 'now'},
                        timepicker={'hidden': True}, panels=[], templating={'list': [
                            query_variable('refresh_id', 'Bản dữ liệu',
                                           'SELECT toString(refresh_id) FROM ops.mart_refreshes ORDER BY refresh_id DESC LIMIT 1', hidden=True),
                            query_variable('business_day', 'Ngày nghiệp vụ',
                                           f'SELECT toString(max(order_date)) FROM mart.daily_sales WHERE {SNAPSHOT}', hidden=True),
                        ]}, links=[dict(title=name, type='link', url='/d/' + target,
                                        targetBlank=False, includeVars=True, keepTime=False)
                                   for target, name in NAV])
        self.text('MARKETPLACE / ' + title,
                  f'### {purpose}\nNgày nghiệp vụ **${{business_day}}** · Chọn góc nhìn qua các liên kết phía trên. '
                  'Số liệu phản ánh trạng thái hiện tại, có thể đổi khi đơn bị hủy hoặc hoàn trả.', h=4)

    def variable(self, variable):
        self.doc['templating']['list'].append(variable)

    def add(self, title, query=None, kind='table', x=0, w=24, h=8, unit='short',
            description='', color='blue', overrides=None, options=None):
        p = dict(id=len(self.doc['panels']) + 1, title=title, type=kind,
                 gridPos=dict(x=x, y=self.y, w=w, h=h), description=description,
                 fieldConfig={'defaults': {'unit': unit, 'color': {'mode': 'palette-classic'},
                                           'noValue': 'Chưa có dữ liệu'}, 'overrides': overrides or []},
                 options=options or {})
        if query:
            p.update(datasource=deepcopy(CH), targets=[sql(query)])
        if kind == 'stat':
            p['fieldConfig']['defaults'].update(color={'mode': 'fixed', 'fixedColor': COLORS[color]})
            p['options'] = dict(reduceOptions=dict(calcs=['lastNotNull'], fields='', values=False),
                                colorMode='value', graphMode='none', textMode='auto',
                                justifyMode='auto', orientation='auto', wideLayout=True)
        elif kind == 'table':
            p['options'].update(showHeader=True, cellHeight='sm', footer={'show': False})
            p['fieldConfig']['defaults']['custom'] = {'minWidth':65,'wrapText':True}
        self.doc['panels'].append(p)
        return p

    def text(self, title, content, h=3):
        self.add(title, kind='text', h=h, options={'mode': 'markdown', 'content': content})
        self.y += h

    def section(self, title):
        self.add(title, kind='row', h=1, options={})['collapsed'] = False
        self.y += 1

    def stats(self, specs):
        width = 24 // len(specs)
        for index, spec in enumerate(specs):
            title, query, unit, color, description = spec
            self.add(title, query, kind='stat', x=index * width, w=width, h=4,
                     unit=unit, color=color, description=description)
        self.y += 4

    def bars(self, title, query, axis, x=0, w=12, h=8, unit='short', horizontal=False,
             stacked=False, description='', overrides=None):
        semantic_colors = {'GMV': 'blue', 'NMV': 'green', 'Xác nhận / đặt': 'blue', 'NMV / GMV': 'green',
                           'Chưa xác nhận': 'amber', 'Hủy / trả sau xác nhận': 'red', 'Đơn đặt': 'blue',
                           'Đơn có ưu đãi': 'purple', 'Đơn': 'blue', 'Giờ đóng gói TB': 'amber',
                           'Giờ vận chuyển': 'amber', 'Thành công': 'green', 'Thất bại': 'red',
                           'Hết hạn': 'amber', 'Hủy': 'purple', 'Đang chờ': 'blue', 'Đang mở': 'amber',
                           'Đã hoàn': 'purple', 'Đã đóng': 'cyan'}
        overrides = list(overrides or []) + [override(name, color={'mode': 'fixed', 'fixedColor': COLORS[c]}) for name,c in semantic_colors.items()]
        return self.add(title, query, 'barchart', x=x, w=w, h=h, unit=unit,
                        description=description, overrides=overrides,
                        options=dict(xField=axis, orientation='horizontal' if horizontal else 'vertical',
                                     xTickLabelRotation=0 if horizontal else -35,
                                     xTickLabelSpacing=70, showValue='auto' if horizontal else 'never',
                                     stacking='normal' if stacked else 'none', groupWidth=0.7, barWidth=0.85,
                                     tooltip={'mode': 'multi', 'sort': 'desc'},
                                     legend={'displayMode': 'list', 'placement': 'bottom'}))

    def donut(self, title, query, x=0, w=8, description='', unit='short'):
        p = self.add(title, query, 'piechart', x=x, w=w, description=description, unit=unit,
                        options=dict(pieType='donut', displayLabels=['percent'],
                                     reduceOptions={'values': True, 'calcs': ['lastNotNull'], 'fields': ''},
                                     legend={'displayMode': 'table', 'placement': 'right', 'values': ['value', 'percent']},
                                     tooltip={'mode': 'single'}))
        p['fieldConfig']['defaults']['decimals'] = 2
        return p

    def footer(self):
        self.y += 1
        self.add('Độ mới của dữ liệu',
                 f'''SELECT toTimeZone(finished_at, 'Asia/Ho_Chi_Minh') AS "Cập nhật lúc",
                            duration_ms / 1000 AS "Thời gian dựng bản (giây)",
                            marts AS "Số mart đồng bộ"
                     FROM ops.mart_refreshes WHERE {SNAPSHOT} ORDER BY finished_at DESC LIMIT 1''', h=3)
        return self.doc


def override(name, **properties):
    return {'matcher': {'id': 'byName', 'options': name},
            'properties': [{'id': k, 'value': v} for k, v in properties.items()]}


def rate(name, warning=0.1, critical=0.2, good_high=False, width=None):
    colors = ['red', 'orange', 'green'] if good_high else ['green', 'orange', 'red']
    result = override(name, unit='percentunit', decimals=1, color={'mode': 'thresholds'},
                    thresholds={'mode': 'absolute', 'steps': [
                        {'color': colors[0], 'value': None}, {'color': colors[1], 'value': warning},
                        {'color': colors[2], 'value': critical}]},
                    **{'custom.cellOptions': {'type': 'color-background', 'mode': 'basic'}})
    if width is not None:
        result['properties'].append({'id':'custom.width','value':width})
    return result


def data_bar(name, unit='short', color='blue'):
    return override(name, unit=unit, color={'mode': 'fixed', 'fixedColor': COLORS[color]},
                    **{'custom.cellOptions': {'type': 'gauge', 'mode': 'basic'}})


def cell(name, unit='short', decimals=None):
    args = {'unit': unit}
    if decimals is not None:
        args['decimals'] = decimals
    return override(name, **args)


def overview():
    b = Board('cdc-business', '01 · Điều hành sàn', 'Sàn tăng trưởng ở đâu — và giá trị đang thất thoát ở khâu nào?')
    b.variable(custom_variable('period_days', 'Cửa sổ ngày đã qua', [7, 14, 30], 7))
    b.section('01 / HIỆU QUẢ BÁN HÀNG · ${period_days} ngày trước ${business_day}')
    base = f'FROM mart.daily_sales WHERE {SNAPSHOT} AND {PERIOD}'
    note = 'Theo ngày đặt đơn, bỏ ngày đang chạy để không so ngày dở dang với ngày đầy đủ. Đây là số live, không phải báo cáo đã chốt.'
    b.stats([
        ('GMV xác nhận', f'SELECT sum(gmv) {base}', 'currencyVND', 'blue', note),
        ('NMV giữ lại', f'SELECT sum(nmv) {base}', 'currencyVND', 'green', note),
        ('Đơn đã đặt', f'SELECT sum(orders) {base}', 'short', 'purple', note),
        ('AOV', f'SELECT sum(gmv)/nullIf(sum(confirmed_orders),0) {base}', 'currencyVND', 'cyan', 'GMV / số đơn đã xác nhận. ' + note),
        ('NMV / GMV', f'SELECT sum(nmv)/nullIf(sum(gmv),0) {base}', 'percentunit', 'green', 'NMV / GMV. Bao gồm ảnh hưởng hủy sau xác nhận, trả về người bán và hoàn hàng.'),
        ('Đơn bị hủy', f'SELECT sum(cancelled_orders)/nullIf(sum(orders),0) {base}', 'percentunit', 'amber', 'Gồm cả đơn hết hạn chưa thanh toán; không phải toàn bộ lỗi của nhà bán.'),
    ])
    b.bars('Giá trị qua các ngày · GMV vs NMV',
           f'SELECT toString(order_date) AS "Ngày", gmv AS "GMV", nmv AS "NMV" {base} ORDER BY order_date',
           'Ngày', w=14, unit='currencyVND', description=note)
    b.add('So với ${period_days} ngày liền trước', f'''
        SELECT 'GMV' AS "Chỉ tiêu", sumIf(gmv, {PERIOD}) AS "Kỳ chọn", sumIf(gmv, {PREVIOUS}) AS "Kỳ trước",
               if(min(order_date) <= {ANCHOR} - INTERVAL (${{period_days:raw}}*2) DAY,
                  (sumIf(gmv,{PERIOD})-sumIf(gmv,{PREVIOUS}))/nullIf(sumIf(gmv,{PREVIOUS}),0),NULL) AS "Thay đổi"
        FROM mart.daily_sales WHERE {SNAPSHOT}
        UNION ALL SELECT 'NMV',sumIf(nmv,{PERIOD}),sumIf(nmv,{PREVIOUS}),
               if(min(order_date) <= {ANCHOR} - INTERVAL (${{period_days:raw}}*2) DAY,
                  (sumIf(nmv,{PERIOD})-sumIf(nmv,{PREVIOUS}))/nullIf(sumIf(nmv,{PREVIOUS}),0),NULL)
        FROM mart.daily_sales WHERE {SNAPSHOT}''', x=14, w=10,
        description='Hai cửa sổ cùng số ngày. NMV kỳ mới còn cohort chưa hết cửa sổ trả hàng, chưa phải kết quả cuối cùng. Thay đổi để trống nếu chưa đủ lịch sử hoặc mẫu số bằng 0.',
        overrides=[override('Chỉ tiêu',**{'custom.width':80}),override('Kỳ chọn',unit='currencyVND',**{'custom.width':100}),override('Kỳ trước',unit='currencyVND',**{'custom.width':100}),rate('Thay đổi',0,0.1,True,width=100)])
    b.y += 8
    b.bars('Cơ hội chưa xác nhận & giá trị chưa giữ lại', f'''
        SELECT toString(order_date) AS "Ngày", greatest(placed_gmv-gmv,0) AS "Chưa xác nhận",
               greatest(gmv-nmv,0) AS "Hủy / trả sau xác nhận" {base} ORDER BY order_date''',
        'Ngày', unit='currencyVND', stacked=True, description='Hai phần chênh lệch tại thời điểm báo cáo; không gọi là lỗ hay lợi nhuận. Đơn mới còn có thể được xác nhận.')
    b.bars('Tỷ lệ xác nhận & tỷ lệ giữ lại theo ngày', f'''
        SELECT toString(order_date) AS "Ngày", confirmed_orders/nullIf(orders,0) AS "Xác nhận / đặt",
               nmv/nullIf(gmv,0) AS "NMV / GMV" {base} ORDER BY order_date''',
        'Ngày', x=12, unit='percentunit', description=note)
    b.y += 8
    b.section('02 / NHỊP KINH DOANH HIỆN TẠI · ngày ${business_day} đang diễn ra')
    today = f'FROM mart.daily_sales WHERE {SNAPSHOT} AND order_date={ANCHOR}'
    b.stats([
        ('Đơn hôm nay · chưa đủ ngày',f'SELECT sum(orders) {today}','short','purple','Không so trực tiếp với tổng cả ngày hôm qua.'),
        ('GMV hôm nay · đang cập nhật',f'SELECT sum(gmv) {today}','currencyVND','blue','Ngày đang diễn ra, còn đơn trong cửa sổ thanh toán.'),
        ('NMV hôm nay · đang cập nhật',f'SELECT sum(nmv) {today}','currencyVND','green','Cohort mới, chưa đủ thời gian phát sinh trả hàng.'),
        ('Người mua · toàn lịch sử',f'SELECT buyers FROM mart.overview_kpis WHERE {SNAPSHOT}','short','cyan','Distinct người mua có đơn confirmed, toàn bộ lịch sử. Không cộng buyers theo ngày vì sẽ đếm trùng.'),
    ])
    b.bars('48 giờ nghiệp vụ gần nhất · chuẩn bị năng lực theo nhịp đơn', f'''
        SELECT formatDateTime(hour,'%d/%m %Hh','Asia/Ho_Chi_Minh') AS "Giờ nghiệp vụ",
               orders AS "Đơn đặt", orders_with_voucher AS "Đơn có ưu đãi"
        FROM mart.traffic_by_hour WHERE {SNAPSHOT} ORDER BY hour''', 'Giờ nghiệp vụ', w=24,
        description='Giờ cuối có thể chưa đầy đủ. Đơn có ưu đãi là tập con của đơn đặt; hai chuỗi không cộng chồng.')
    b.y += 8
    b.section('03 / ƯU TIÊN ĐIỀU HÀNH · tín hiệu tại snapshot, toàn lịch sử')
    b.add('Từ số liệu đến người chịu trách nhiệm', f'''
        SELECT 'Kho vận' AS "Đội phụ trách", 'SKU cần bổ sung' AS "Tín hiệu",
               countIf(needs_reorder) AS "Số lượng", 'Mở Nhà bán & tồn kho; ưu tiên SKU có khoảng thiếu lớn.' AS "Hành động"
        FROM mart.inventory_risk WHERE {SNAPSHOT}
        UNION ALL SELECT 'Seller success','Shop cần rà SLA / hủy',countIf(orders>=30 AND (sla_breaches>0 OR cancel_rate>=0.2)),
               'Xem hàng chờ can thiệp; tách lỗi SLA và hủy chưa thanh toán trước khi xử lý shop.'
        FROM mart.shop_performance WHERE {SNAPSHOT}
        UNION ALL SELECT 'Chăm sóc khách hàng','Yêu cầu trả hàng đang mở',sum(open),
               'Mở Đơn hàng & trải nghiệm; phân bổ người xử lý theo lý do trả hàng.'
        FROM mart.returns_summary WHERE {SNAPSHOT}
        UNION ALL SELECT 'Fulfillment','Đơn chờ shop đóng gói',sumIf(orders,status IN ('TO_SHIP','READY_TO_SHIP')),
               'Theo dõi năng lực xử lý; mart chưa có tuổi từng đơn nên chưa kết luận quá hạn.'
        FROM mart.order_status_summary WHERE {SNAPSHOT}''',
        h=7, overrides=[data_bar('Số lượng')], description='Ngưỡng shop 30 đơn và tỷ lệ hủy 20% là bộ lọc khám phá, không phải chính sách phạt.')
    b.y += 7
    return b.footer()


def operations():
    b=Board('cdc-operations','02 · Đơn hàng & trải nghiệm','Điểm nghẽn nằm ở shop, hãng vận chuyển hay quy trình trả hàng?')
    b.text('Phạm vi', '**Toàn bộ đơn / kiện tại snapshot** · Số đang chờ là backlog hiện tại; tỷ lệ giao chỉ tính kiện đã kết thúc. Cohort trả hàng chưa hết cửa sổ được tách riêng.', h=2)
    b.section('01 / KHỐI LƯỢNG CẦN THEO DÕI')
    status=f'FROM mart.order_status_summary WHERE {SNAPSHOT}'
    b.stats([
        ('Chờ thanh toán',f"SELECT sumIf(orders,status='UNPAID') {status}",'short','amber',ALL_SCOPE),
        ('Chờ shop / bàn giao',f"SELECT sumIf(orders,status IN ('TO_SHIP','READY_TO_SHIP')) {status}",'short','blue','Không có tuổi đơn trong mart nên không đồng nghĩa quá SLA.'),
        ('Đang vận chuyển',f"SELECT sumIf(orders,status IN ('SHIPPED','DELIVERY_FAILED')) {status}",'short','purple',ALL_SCOPE),
        ('Trả hàng đang mở',f'SELECT sum(open) FROM mart.returns_summary WHERE {SNAPSHOT}','short','red','Tổng yêu cầu chưa REFUNDED / CLOSED; không phải số yêu cầu quá hạn.'),
    ])
    b.bars('Hàng chờ ở khâu nào? · loại đơn đã kết thúc',f'''SELECT transform(status,
        ['UNPAID','TO_SHIP','READY_TO_SHIP','SHIPPED','DELIVERY_FAILED','DELIVERED'],
        ['Chờ thanh toán','Chờ đóng gói','Chờ bàn giao','Đang giao','Giao thất bại, chờ xử lý','Đã giao, chờ hoàn tất'],status) AS "Trạng thái", orders AS "Đơn"
        {status} AND status IN ('UNPAID','TO_SHIP','READY_TO_SHIP','SHIPPED','DELIVERY_FAILED','DELIVERED') ORDER BY orders DESC''',
        'Trạng thái',horizontal=True,description='Loại CANCELLED, COMPLETED và RETURN_REFUND; nhóm trả hàng được theo dõi riêng bằng open requests. Không suy ra quá hạn khi thiếu tuổi đơn.')
    b.donut('Kết quả giao hàng · chỉ kiện kết thúc',f'''
        SELECT 'Giao thành công' AS "Kết quả",sum(delivered) AS "Kiện" FROM mart.delivery_performance WHERE {SNAPSHOT}
        UNION ALL SELECT 'Trả về người gửi',sum(returned_to_sender) FROM mart.delivery_performance WHERE {SNAPSHOT}''',x=12,w=12,
        description='Chỉ kiện chiều đi. Kiện chưa kết thúc không nằm trong mẫu số.')
    b.y+=8
    b.section('02 / HÃNG VẬN CHUYỂN · tốc độ, chất lượng và quy mô phải đọc cùng nhau')
    b.bars('Thời gian vận chuyển trung bình · giờ nghiệp vụ',f'''
        SELECT carrier AS "Hãng",avg_hours_in_transit AS "Giờ vận chuyển"
        FROM mart.delivery_performance WHERE {SNAPSHOT} AND delivered>0 ORDER BY avg_hours_in_transit DESC''',
        'Hãng',unit='suffix:giờ',horizontal=True,description='Chỉ các kiện đã giao và có mốc thời gian; không phải SLA cam kết hay p95.')
    b.add('Scorecard hãng vận chuyển',f'''
        SELECT carrier AS "Hãng",parcels AS "Tổng kiện",delivered AS "Đã giao",
               returned_to_sender AS "Trả về",failed_attempts AS "Lượt giao thất bại",
               success_rate AS "Tỷ lệ giao",avg_hours_in_transit AS "Giờ TB"
        FROM mart.delivery_performance WHERE {SNAPSHOT} ORDER BY returned_to_sender DESC,parcels DESC''',x=12,w=12,
        overrides=[data_bar('Tổng kiện'),rate('Tỷ lệ giao',0.9,0.97,True),cell('Giờ TB','suffix:giờ')],
        description='Tỷ lệ giao = delivered / (delivered + returned_to_sender). Lượt giao thất bại có thể lặp trên một kiện; không chia trực tiếp thành % số đơn.')
    b.y+=8
    b.section('03 / TRẢ HÀNG · phân biệt cohort mới và cohort đã hết cửa sổ')
    b.stats([
        ('Tỷ lệ yêu cầu trả · cohort chín',f'SELECT sumIf(returned,matured)/nullIf(sumIf(delivered,matured),0) FROM mart.return_rate_by_cohort WHERE {SNAPSHOT}','percentunit','amber','Tổng đơn có yêu cầu trả / tổng đã giao, chỉ cohort matured. Gồm yêu cầu bị từ chối, đang mở và đã hoàn.'),
        ('Đã hoàn theo yêu cầu trả',f'SELECT sum(refunded) FROM mart.returns_summary WHERE {SNAPSHOT}','short','purple',ALL_SCOPE),
        ('Đã đóng / từ chối',f'SELECT sum(rejected) FROM mart.returns_summary WHERE {SNAPSHOT}','short','cyan',ALL_SCOPE),
        ('Cohort chưa hết cửa sổ',f'SELECT countIf(NOT matured) FROM mart.return_rate_by_cohort WHERE {SNAPSHOT}','short','blue','Các ngày giao hàng còn cửa sổ trả. Không dùng tỷ lệ thấp của nhóm này để kết luận chất lượng tốt.'),
    ])
    b.bars('Vì sao khách yêu cầu trả hàng?',f'''
        SELECT reason AS "Lý do",open AS "Đang mở",refunded AS "Đã hoàn",rejected AS "Đã đóng"
        FROM mart.returns_summary WHERE {SNAPSHOT} ORDER BY returns DESC''','Lý do',horizontal=True,stacked=True)
    b.add('Cohort giao hàng · theo dõi 21 ngày gần nhất',f'''
        SELECT delivery_date AS "Ngày giao",delivered AS "Đã giao",returned AS "Yêu cầu trả",
               return_rate AS "Tỷ lệ yêu cầu trả",if(matured,'Đã hết cửa sổ','Còn cửa sổ trả') AS "Độ chín"
        FROM mart.return_rate_by_cohort WHERE {SNAPSHOT} ORDER BY delivery_date DESC LIMIT 21''',x=12,w=12,
        overrides=[rate('Tỷ lệ yêu cầu trả',0.05,0.1),data_bar('Đã giao')],
        description='Ngưỡng 5% / 10% là tín hiệu khám phá. Đọc cột độ chín và số mẫu trước khi so sánh.')
    b.y+=8
    b.text('Hành động & phần cần bổ sung', '**Hành động:** liên hệ hãng có tỷ lệ trả về tăng; rà shop đóng gói chậm; phân công CS theo lý do trả hàng. '
           '**Chưa có trong mart:** tuổi từng đơn, deadline giao/return, mã tracking và hàng chờ từng case. Dashboard không gắn nhãn “quá hạn” khi chưa có dữ liệu đó.',h=3)
    return b.footer()


def growth():
    b=Board('cdc-growth','03 · Danh mục & chiến dịch','Nên đẩy ngành hàng nào, ưu đãi nào đang được dùng và chiến dịch nào cần rà lại?')
    b.variable(custom_variable('top_n','Top sản phẩm / chiến dịch',[5,10,20],10))
    b.variable(query_variable('category','Ngành lá · chỉ lọc sản phẩm',f'SELECT DISTINCT category FROM mart.product_sales WHERE {SNAPSHOT} ORDER BY category',multi=True))
    b.variable(query_variable('voucher_scope','Bên tài trợ · chỉ lọc voucher',f'SELECT DISTINCT scope FROM mart.voucher_performance WHERE {SNAPSHOT} ORDER BY scope',multi=True))
    product=f'FROM mart.product_sales WHERE {SNAPSHOT} AND category IN (${{category:sqlstring}})'
    voucher=f'FROM mart.voucher_performance WHERE {SNAPSHOT} AND scope IN (${{voucher_scope:sqlstring}})'
    b.section('01 / CƠ CẤU DOANH SỐ · toàn lịch sử tại snapshot')
    b.bars('Ngành gốc nào đóng góp giá trị hàng ròng?',f'''
        SELECT category AS "Ngành gốc",net_revenue AS "Giá trị hàng ròng"
        FROM mart.category_sales WHERE {SNAPSHOT} ORDER BY net_revenue DESC LIMIT 10''','Ngành gốc',w=16,horizontal=True,unit='currencyVND',
        description='Giá trị hàng sau voucher và trả hàng, không gồm ship; có cả COD chưa thu tiền. Đây không phải dòng tiền đã thu hay doanh thu hoa hồng của sàn. Bộ lọc ngành lá không áp dụng biểu đồ ngành gốc.')
    b.donut('Tỷ trọng tiền hàng theo ngành gốc',f'''
        SELECT if(rn<=5,category,'Các ngành còn lại') AS "Ngành",sum(net_revenue) AS "Tiền hàng"
        FROM (SELECT category,net_revenue,row_number() OVER (ORDER BY net_revenue DESC) AS rn
              FROM mart.category_sales WHERE {SNAPSHOT}) GROUP BY "Ngành" ORDER BY "Tiền hàng" DESC''',x=16,w=8,unit='currencyVND')
    b.y+=8
    b.section('02 / SẢN PHẨM · bộ lọc ngành lá áp dụng phần này')
    b.stats([
        ('Giá trị hàng · nhóm chọn',f'SELECT sum(net_revenue) {product}','currencyVND','green','Sau voucher và return refunds; confirmed và kept, gồm COD có thể chưa thu tiền. '+ALL_SCOPE),
        ('Số lượng giữ lại',f'SELECT sum(units_sold) {product}','short','blue','Đơn vị hàng sau khi trừ số lượng đã hoàn tiền.'),
        ('Nhóm sản phẩm có doanh số',f'SELECT count() {product}','short','purple','Grain là tên sản phẩm × ngành lá × shop, không phải distinct product_id.'),
    ])
    b.bars('Top ${top_n} sản phẩm · ưu tiên merchandising',f'''
        SELECT concat(substringUTF8(product_name,1,34),' · ',substringUTF8(shop,1,20)) AS "Sản phẩm · shop",
               net_revenue AS "Giá trị hàng ròng" {product} ORDER BY net_revenue DESC LIMIT ${{top_n:raw}}''',
        'Sản phẩm · shop',w=12,horizontal=True,unit='currencyVND')
    b.add('Danh sách sản phẩm để làm việc với shop',f'''
        SELECT product_name AS "Sản phẩm",shop AS "Shop",units_sold AS "SL giữ lại",net_revenue AS "Giá trị hàng ròng"
        {product} ORDER BY net_revenue DESC LIMIT ${{top_n:raw}}''',x=12,w=12,
        overrides=[data_bar('Giá trị hàng ròng','currencyVND'),override('Sản phẩm',**{'custom.width':170}),override('Shop',**{'custom.width':170}),override('SL giữ lại',**{'custom.width':90})],
        description='Xếp hạng trong ngành lá đang chọn. Tiền hàng sau voucher và hoàn trả; một sản phẩm có thể xuất hiện ở nhiều shop.')
    b.y+=8
    b.section('03 / VOUCHER · bên tài trợ được chọn, chỉ lượt APPLIED')
    b.stats([
        ('Lượt dùng đang hiệu lực',f'SELECT sum(redemptions) {voucher}','short','blue','Không tính lượt RELEASED.'),
        ('Ưu đãi đã áp dụng',f'SELECT sum(discount_cost) {voucher}','currencyVND','amber','Voucher mart chưa cùng cơ sở lọc confirmed/kept với platform_pnl; không bắt buộc hai tổng khớp.'),
        ('Ưu đãi / lượt dùng',f'SELECT sum(discount_cost)/nullIf(sum(redemptions),0) {voucher}','currencyVND','purple','Chi phí bình quân trên một redemption; không phải CAC hoặc ROI.'),
    ])
    b.bars('Voucher nào đang hấp thụ ngân sách?',f'SELECT code AS "Voucher",discount_cost AS "Giá trị ưu đãi" {voucher} ORDER BY discount_cost DESC LIMIT ${{top_n:raw}}','Voucher',horizontal=True,unit='currencyVND')
    b.add('Quota & hiệu quả sử dụng voucher',f'''
        SELECT code AS "Mã",scope AS "Tài trợ",kind AS "Loại",redemptions AS "Đã dùng",usage_limit AS "Giới hạn",
               redemption_rate AS "Dùng / quota",discount_cost AS "Ưu đãi"
        {voucher} ORDER BY discount_cost DESC LIMIT ${{top_n:raw}}''',x=12,w=12,
        overrides=[rate('Dùng / quota',0.5,0.8,True),data_bar('Ưu đãi','currencyVND')],
        description='Quota dùng thấp không tự chứng minh hiệu quả kém. Chưa có uplift, attribution hay nhóm đối chứng để kết luận ROI.')
    b.y+=8
    b.section('04 / FLASH SALE · phân biệt chưa bắt đầu với đã tới giờ')
    b.bars('Sell-through · ${top_n} chiến dịch mới nhất đã tới giờ',f'''
        SELECT concat(substringUTF8(campaign,1,38),' · ',formatDateTime(starts_at,'%d/%m','Asia/Ho_Chi_Minh')) AS "Chiến dịch",
               sell_through AS "Đã bán / quota"
        FROM mart.flash_sale_performance WHERE {SNAPSHOT}
          AND starts_at <= (SELECT max(hour) FROM mart.traffic_by_hour WHERE {SNAPSHOT})
        ORDER BY starts_at DESC LIMIT ${{top_n:raw}}''','Chiến dịch',horizontal=True,unit='percentunit',
        description='Mốc tham chiếu là đầu giờ đặt đơn mới nhất, bảo thủ tới một giờ. Không có ends_at nên không kết luận chiến dịch đang chạy hay đã kết thúc.')
    b.add('Lịch chiến dịch & cơ hội kiểm tra quota',f'''
        SELECT campaign AS "Chiến dịch",kind AS "Loại",toTimeZone(starts_at,'Asia/Ho_Chi_Minh') AS "Bắt đầu",
               sold AS "Đã bán",quota AS "Quota",sell_through AS "Sell-through",revenue_at_flash_price AS "Giá trị theo giá flash",
               if(starts_at>(SELECT max(hour) FROM mart.traffic_by_hour WHERE {SNAPSHOT}),'Chưa tới mốc giờ tham chiếu','Đã tới giờ bắt đầu') AS "Mốc thời gian"
        FROM mart.flash_sale_performance WHERE {SNAPSHOT} ORDER BY starts_at DESC LIMIT ${{top_n:raw}}''',x=12,w=12,
        overrides=[rate('Sell-through',0.3,0.7,True),cell('Giá trị theo giá flash','currencyVND')],
        description='Giá trị sold × sale_price, không phải tiền thực thu sau voucher/return. Sell-through thấp của chiến dịch tương lai không phải cảnh báo.')
    b.y+=8
    b.section('05 / KHÁCH HÀNG · cơ cấu hạng thành viên, toàn sàn')
    b.donut('Cơ cấu khách hàng theo hạng',f'SELECT tier AS "Hạng",total_customers AS "Khách" FROM mart.customer_tiers WHERE {SNAPSHOT} ORDER BY total_customers DESC',w=10,
            description='Toàn bộ hồ sơ khách theo hạng hiện tại; không phải retention cohort hoặc phân khúc tại thời điểm mua.')
    b.add('Nền khách để lập kế hoạch chăm sóc',f'''SELECT tier AS "Hạng",total_customers AS "Khách",active_customers AS "Đang hoạt động",
               lifetime_spent AS "Chi tiêu tích lũy",avg_spent AS "Chi tiêu TB / khách"
        FROM mart.customer_tiers WHERE {SNAPSHOT} ORDER BY lifetime_spent DESC''',x=10,w=14,
        overrides=[data_bar('Chi tiêu tích lũy','currencyVND'),cell('Chi tiêu TB / khách','currencyVND')],
        description='is_active là cờ hồ sơ, không phải active trong 7/30 ngày. total_spent không phải CLV dự báo; muốn thiết kế retention cần lịch sử giao dịch theo khách.')
    b.y+=8
    return b.footer()


def sellers():
    b=Board('cdc-sellers','04 · Nhà bán & tồn kho','Shop nào cần hỗ trợ trước — và SKU nào cần bổ sung để tránh bỏ lỡ đơn?')
    b.variable(query_variable('tier','Hạng shop',f'SELECT DISTINCT tier FROM mart.shop_performance WHERE {SNAPSHOT} ORDER BY tier',multi=True))
    b.variable(query_variable('province','Tỉnh / thành',f'SELECT DISTINCT province FROM mart.shop_performance WHERE {SNAPSHOT} AND tier IN (${{tier:sqlstring}}) ORDER BY province',multi=True))
    b.variable(query_variable('shop','Nhà bán',f'SELECT DISTINCT shop FROM mart.shop_performance WHERE {SNAPSHOT} AND tier IN (${{tier:sqlstring}}) AND province IN (${{province:sqlstring}}) ORDER BY shop',multi=True))
    b.variable(custom_variable('min_orders','Số đơn tối thiểu khi xếp rủi ro',[10,30,100,300],30))
    shops=f'FROM mart.shop_performance WHERE {SNAPSHOT} AND tier IN (${{tier:sqlstring}}) AND province IN (${{province:sqlstring}}) AND shop IN (${{shop:sqlstring}})'
    # Inventory contains shops with no orders too. Restrict explicitly to this selection's shop-performance universe.
    inventory=f'FROM mart.inventory_risk WHERE {SNAPSHOT} AND shop IN (SELECT shop {shops})'
    b.section('01 / DANH MỤC NHÀ BÁN ĐƯỢC CHỌN · số tích lũy')
    b.stats([
        ('Shop có đơn',f'SELECT count() {shops}','short','blue','Chỉ các shop xuất hiện trong shop_performance, tức có đơn. Shop chưa bán hàng không nằm trong danh sách này.'),
        ('NMV của nhóm shop',f'SELECT sum(nmv) {shops}','currencyVND','green',ALL_SCOPE),
        ('Lần vi phạm SLA đóng gói',f'SELECT sum(sla_breaches) {shops}','short','red','Đơn bị hủy vì seller_sla_breach. Đây là vi phạm đã xảy ra, không phải backlog sắp quá hạn.'),
        ('SKU chạm ngưỡng bổ sung',f'SELECT countIf(needs_reorder) {inventory}','short','amber','Tồn available <= reorder_point, trong nhóm shop có đơn được chọn.'),
    ])
    b.bars('Nhà bán đóng góp lớn · GMV và phần giữ lại',f'SELECT shop AS "Shop",gmv AS "GMV",nmv AS "NMV" {shops} ORDER BY nmv DESC LIMIT 10','Shop',w=14,horizontal=True,unit='currencyVND')
    b.bars('Đóng gói chậm · chỉ shop đủ số mẫu đơn',f'''
        SELECT shop AS "Shop",avg_hours_to_pack AS "Giờ đóng gói TB" {shops}
        AND orders>=${{min_orders:raw}} AND avg_hours_to_pack IS NOT NULL ORDER BY avg_hours_to_pack DESC LIMIT 10''',
        'Shop',x=14,w=10,horizontal=True,unit='suffix:giờ',
        description='Trung bình chỉ trên đơn đã đóng gói. Số đơn tối thiểu lọc theo tổng đơn shop; mart chưa có số mẫu packed để lọc riêng.')
    b.y+=8
    b.section('02 / HÀNG CHỜ CAN THIỆP · hỗ trợ có căn cứ, không tự động phạt')
    b.add('Shop cần rà soát SLA / hủy · tối thiểu ${min_orders} đơn',f'''
        SELECT shop AS "Nhà bán",tier AS "Hạng",province AS "Tỉnh",orders AS "Đơn",
               sla_breaches AS "Vi phạm SLA",cancel_rate AS "Tỷ lệ hủy",avg_hours_to_pack AS "Giờ đóng gói TB",
               rating AS "Sao",reviews AS "Lượt đánh giá",
               multiIf(sla_breaches>0,'Rà nguyên nhân vi phạm SLA',avg_hours_to_pack>=24,'Kiểm tra năng lực đóng gói','Phân tích hủy / hết hạn thanh toán') AS "Bước tiếp theo"
        {shops} AND orders>=${{min_orders:raw}} AND (sla_breaches>0 OR cancel_rate>=0.2 OR avg_hours_to_pack>=24)
        ORDER BY sla_breaches DESC,cancel_rate DESC LIMIT 20''',h=9,
        overrides=[override('Nhà bán',**{'custom.width':180}),override('Bước tiếp theo',**{'custom.width':240}),data_bar('Vi phạm SLA',color='red'),rate('Tỷ lệ hủy',0.1,0.2),cell('Giờ đóng gói TB','suffix:giờ'),cell('Sao','short',2)],
        description='20% hủy và 24h đóng gói là ngưỡng khám phá, không phải quy định xử phạt. Cancel rate còn bao gồm hủy do người mua / chưa thanh toán; đọc cùng số mẫu và SLA thực.')
    b.y+=9
    b.section('03 / BỔ SUNG TỒN KHO · chênh lệch tới ngưỡng, chưa phải dự báo nhu cầu')
    b.add('SKU cần làm việc với shop',f'''
        SELECT sku AS "SKU",product_name AS "Sản phẩm",shop AS "Nhà bán",variant AS "Biến thể",
               available AS "Có thể bán",reserved AS "Đang giữ",reorder_point AS "Ngưỡng",
               greatest(reorder_point+1-available,0) AS "Bổ sung tối thiểu để vượt ngưỡng"
        {inventory} AND needs_reorder
        ORDER BY "Bổ sung tối thiểu để vượt ngưỡng" DESC,available ASC LIMIT 30''',h=9,
        overrides=[override('Sản phẩm',**{'custom.width':180}),override('Nhà bán',**{'custom.width':180}),override('Bổ sung tối thiểu để vượt ngưỡng',**{'custom.width':240}),data_bar('Bổ sung tối thiểu để vượt ngưỡng',color='amber'),cell('Có thể bán'),cell('Đang giữ')],
        description='Ưu tiên theo khoảng thiếu tới ngưỡng. Không gọi đây là days-of-cover, dự báo bán hoặc lượng mua tối ưu: mart chưa có demand theo SKU / lead time.')
    b.y+=9
    b.text('Giới hạn bộ lọc', 'Bộ lọc nhà bán chỉ gồm shop đã có đơn. Shop chưa phát sinh đơn vẫn có thể có tồn kho nhưng không nằm trong góc nhìn này. '
           'Tên shop là grain hiện có của mart; drill-down tới order_id / SKU demand cần mart chi tiết hơn.',h=3)
    return b.footer()


def finance():
    b=Board('cdc-finance','05 · Thu phí & dòng tiền','Tiền đến từ đâu, đang hoàn vì lý do gì và phần nào còn ở ví người bán?')
    b.variable(query_variable('payment_method','Phương thức · chỉ lọc thanh toán',f'SELECT DISTINCT method FROM mart.payment_summary WHERE {SNAPSHOT} ORDER BY method',multi=True))
    pnl=f'FROM mart.platform_pnl WHERE {SNAPSHOT}'
    pay=f'FROM mart.payment_summary WHERE {SNAPSHOT} AND method IN (${{payment_method:sqlstring}})'
    b.section('01 / THU PHÍ & NGÂN SÁCH SÀN · toàn lịch sử, chưa phải báo cáo lợi nhuận')
    b.stats([
        ('Hoa hồng + phí thanh toán',f'SELECT take {pnl}','currencyVND','green','Take từ seller ledger, phát sinh khi đơn hoàn tất; không phải GMV.'),
        ('Take rate · GMV hoàn tất',f'SELECT take_rate {pnl}','percentunit','blue','Mẫu số là GMV của đơn COMPLETED, không phải GMV toàn sàn.'),
        ('Ưu đãi sàn đã gánh',f'SELECT platform_promo_cost {pnl}','currencyVND','amber','Voucher sàn + trợ giá ship trên confirmed và kept. Chưa phải toàn bộ chi phí vận hành.'),
        ('Đã chi trả người bán',f'SELECT paid_to_sellers {pnl}','currencyVND','purple','Tổng seller payouts; thời điểm ghi nhận khác thời điểm GMV / take.'),
    ])
    b.bars('Quy mô các khoản tiền · đọc riêng từng cơ sở ghi nhận',f'''
        SELECT 'Thu phí sàn' AS "Khoản",take AS "VND" {pnl}
        UNION ALL SELECT 'Ưu đãi sàn',platform_promo_cost {pnl}
        UNION ALL SELECT 'Ưu đãi shop',seller_promo_cost {pnl}
        UNION ALL SELECT 'Hoàn do trả hàng',return_refunds {pnl}''','Khoản',w=14,horizontal=True,unit='currencyVND',
        description='Các khoản khác cơ sở thời điểm / trạng thái. Không cộng trừ trực tiếp để gọi là lợi nhuận; không phải waterfall kế toán.')
    b.donut('Hoàn tiền theo nguyên nhân',f'''
        SELECT 'Trả hàng' AS "Nguyên nhân",return_refunds AS "VND" {pnl}
        UNION ALL SELECT 'Hủy / giao thất bại',cancellation_refunds {pnl}
        UNION ALL SELECT 'Thanh toán trễ',late_payment_refunds {pnl}''',x=14,w=10,
        unit='currencyVND', description='Hoàn hủy và thanh toán trễ là trả lại tiền, không đồng nghĩa chi phí bán hàng. Hoàn trễ lớn nên rà gateway / timeout trước khi tăng khuyến mãi.')
    b.y+=8
    b.section('02 / THANH TOÁN · áp dụng phương thức được chọn')
    b.stats([
        ('Thành công / lượt đã kết thúc',f'SELECT sum(succeeded)/nullIf(sum(attempts-pending),0) {pay}','percentunit','green','Đếm payment attempts, không phải distinct checkout; retry làm tăng số lượt. COD được thu theo đơn.'),
        ('Lượt đang chờ',f'SELECT sum(pending) {pay}','short','amber','Trạng thái PENDING không đồng nghĩa đã lỗi hoặc đã quá hạn.'),
        ('Đã thu thành công',f'SELECT sum(collected) {pay}','currencyVND','blue','Tổng payment SUCCEEDED; bao gồm tiền về rồi được hoàn, không phải doanh thu thuần.'),
        ('Đã hoàn tiền',f'SELECT sum(refunded) {pay}','currencyVND','purple','Tổng tiền hoàn theo phương thức, gồm trả hàng / hủy / thanh toán trễ.'),
    ])
    b.bars('Cơ cấu kết quả từng phương thức',f'''
        SELECT method AS "Phương thức",succeeded AS "Thành công",failed AS "Thất bại",expired AS "Hết hạn",
               cancelled AS "Hủy",pending AS "Đang chờ" {pay} ORDER BY attempts DESC''',
        'Phương thức',stacked=True,description='Các trạng thái rời nhau, cùng grain payment attempt. Không suy ra checkout conversion từ số lượt thanh toán.')
    b.add('Đối chiếu tỷ lệ và số tiền theo phương thức',f'''
        SELECT method AS "Phương thức",attempts AS "Lượt",success_rate AS "Tỷ lệ thành công",
               pending AS "Đang chờ",collected AS "Đã thu",refunded AS "Đã hoàn"
        {pay} ORDER BY attempts DESC''',x=12,w=12,
        overrides=[rate('Tỷ lệ thành công',0.8,0.95,True),data_bar('Đã thu','currencyVND'),cell('Đã hoàn','currencyVND')])
    b.y+=8
    b.section('03 / CHECKOUT · toàn sàn, tách biệt với payment attempts')
    b.bars('Checkout tạo mới & đã xác nhận · cùng đơn vị checkout',f'''SELECT stage AS "Giai đoạn", n AS "Checkout"
        FROM mart.funnel WHERE {SNAPSHOT} AND stage IN ('Checkouts','Checkouts paid / COD') ORDER BY _row''',
        'Giai đoạn',horizontal=True,description='Hai tập lồng nhau, không cộng chồng. Một checkout có thể sinh nhiều order và nhiều payment attempts. Bộ lọc phương thức không áp dụng phần này.')
    b.add('Cần rà tiếp điều gì?',f'''SELECT sumIf(n,stage='Checkouts') AS "Checkout tạo",
               sumIf(n,stage='Checkouts paid / COD') AS "Đã xác nhận",
               sumIf(n,stage='Checkouts paid / COD') / nullIf(sumIf(n,stage='Checkouts'),0) AS "Tỷ lệ xác nhận hiện tại"
        FROM mart.funnel WHERE {SNAPSHOT}''',x=12,w=12,
        overrides=[rate('Tỷ lệ xác nhận hiện tại',0.6,0.8,True)],
        description='Tỷ lệ trạng thái tại snapshot, chưa phải conversion cohort đã trưởng thành. Chưa xác nhận còn gồm đang chờ, hết hạn và hủy; không gọi tất cả là bỏ giỏ.')
    b.y+=8
    b.section('04 / VÍ NGƯỜI BÁN · tiền còn lại theo seller ledger, toàn sàn')
    b.bars('Top 10 số dư ví lớn nhất',f'''
        SELECT shop AS "Nhà bán",balance AS "Số dư ví" FROM mart.seller_settlement WHERE {SNAPSHOT}
        ORDER BY balance DESC LIMIT 10''','Nhà bán',horizontal=True,unit='currencyVND',
        description='Số dư ledger; không đồng nghĩa khoản đã đến hạn chi trả. Chưa có payout due-date trong mart.')
    b.add('Đối chiếu doanh số, phí và payout',f'''
        SELECT shop AS "Nhà bán",tier AS "Hạng",sales AS "Ghi có bán hàng",commission AS "Hoa hồng",
               payment_fees AS "Phí thanh toán",paid_out AS "Đã chi",balance AS "Số dư"
        FROM mart.seller_settlement WHERE {SNAPSHOT} ORDER BY balance DESC LIMIT 15''',x=12,w=12,
        overrides=[cell(n,'currencyVND') for n in ['Ghi có bán hàng','Hoa hồng','Phí thanh toán','Đã chi','Số dư']],
        description='Theo ledger: số dư = ghi có bán hàng − hoa hồng − phí thanh toán − đã chi, với các loại bút toán hiện có.')
    b.y+=8
    b.text('Quyết định có thể hỗ trợ', '**Rà payment gateway:** tỷ trọng hoàn do thanh toán trễ. **Rà ngân sách:** ưu đãi theo bên tài trợ. '
           '**Rà đối soát người bán:** số dư, phí và payout. Chưa có chi phí vận hành, attribution marketing, hạn thanh toán nên không kết luận lợi nhuận, ROAS hoặc nợ quá hạn.',h=3)
    return b.footer()


def build_dashboards():
    return [overview(), operations(), growth(), sellers(), finance()]
