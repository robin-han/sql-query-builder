const SortType = {
    Unsorted: 0,
    Ascending: 1,
    Descending: 2
};

const GroupBy = {
    GroupBy: 1,
    Sum: 2,
    Avg: 3,
    Min: 4,
    Max: 5,
    Count: 6,
    Expression: 7,
    Where: 8,
    SumDistinct: 9,
    AvgDistinct: 10,
    MinDistinct: 11,
    MaxDistinct: 12,
    CountDistinct: 13,

    //StDev,
    //StDevP,
    //Var,
    //VarP
};

const JoinType = {
    Inner: 1,
    Left: 2,
    Right: 3,
    Full: 4
};

const Events = {
    PropertyChanged: 'PropertyChanged',
    SqlChanged: 'SqlChanged'
};
const ArrayChangedType = {
    Reset: 0,       // Much of the array changed
    ItemAdded: 1,   // An item added to the array
    ItemDeleted: 2, // An item deleted from the array
    ItemMoved: 3,   // An item moved within the array
    ItemChanged: 4  // An item changed in the array
};

//for format output sql 
const SPACE = ' ';
const TAB = '\t';
//for parsing filter
const filterReg1 = /^([^!<>=]*)\s*(<|>|=|<>|<=|>=|!=)\s*([^<>=!]+)$/i;
const filterReg2 = /^([^!<>=]*)\s*BETWEEN\s+(.+)\s+AND\s+(.+)$/i;
//for parsing column
const exprReg = /^([^\(]*)\((\s*DISTINCT\s+)*(.*)\)$/i;// sum(a)
const columnReg = /^([^\.]*)\.(.*)+$/; // a.b

class Event {
    constructor() {
        this._events = {};
    }

    on(type, fn, context) {
        this._bind(type, fn, false, context);
    }
    once(type, fn, context) {
        this._bind(type, fn, true, context);
    }
    off(type) {
        if (!type) {
            this._events = {};
        }

        let events = this._events;
        let typeStr = typeof (type);
        if (typeStr === 'string') {
            delete events[type];
        } else if (typeStr === 'function') {
            for (let t in events) {
                let fns = events[t];
                for (let i = 0; i < fns.length; i++) {
                    if (fns[i][0] === type) {
                        fns.splice(i--, 1);
                    }
                }
            }
        }
    }

    fire(type, ...args) {
        let fns = this._events[type];
        if (!fns || fns.length === 0) {
            return;
        }

        for (let i = 0; i < fns.length; i++) {
            let fn = fns[i];
            fn[0].apply(fn[2], args);
            if (fn[1]) { //once
                fns.splice(i--, i);
            }
        }
    }

    _bind(type, fn, once, context) {
        if (typeof (type) !== 'string' || typeof (fn) !== 'function') {
            return;
        }

        if (!this._events[type]) {
            this._events[type] = [];
        }
        this._events[type].push([fn, once, context]);
    }
}

class QueryField extends Event {
    constructor(options) {
        super();

        options = options || {};

        this._column = options.column || '';
        //this._columnType = options.columnType || '';
        this._alias = options.alias || '';
        this._table = options.table || '';
        this._output = options.output === false ? false : true;
        this._groupBy = options.groupBy || GroupBy.Expression;
        this._sortType = options.sortType || SortType.Unsorted;
        this._sortOrder = options.sortOrder || 1;
        this._filters = options.filters || [];
    }

    get column() {
        return this._column;
    }
    set column(value) {
        if (this._column !== value) {
            let newColumn = value;

            let m = newColumn.match(exprReg);// sum(a)
            if (m) {
                let agg = m[1];
                agg = agg[0].toUpperCase() + agg.substr(0).toLowerCase();

                let distinct = !m[2];
                if (distinct) {
                    agg += 'Distinct';
                }
                if (GroupBy[agg] !== undefined) {
                    this._groupBy = GroupBy[agg];
                }
                newColumn = m[3];
            }

            m = newColumn.match(columnReg);// a.b
            if (m) {
                this._table = m[1];
                newColumn = m[2];
            }

            this._column = newColumn;
            this._onPropertyChanged('column');
        }
    }

    //get columnType() {
    //    return this._columnType;
    //}
    //set columnType(value) {
    //    if (this._columnType !== value) {
    //        this._columnType = value;
    //    }
    //}

    get alias() {
        return this._alias;
    }
    set alias(value) {
        if (this._alias !== value) {
            this._alias = value;
            this._onPropertyChanged('alias');
        }
    }

    get table() {
        return this._table;
    }
    set table(value) {
        if (this._table !== value) {
            this._table = value;
            this._onPropertyChanged('table');
        }
    }


    get output() {
        return this._output;
    }
    set output(value) {
        if (this._output !== value) {
            this._output = value;
            this._onPropertyChanged('output');
        }
    }

    get groupBy() {
        return this._groupBy;
    }
    set groupBy(value) {
        if (this._groupBy !== value) {
            this._groupBy = value;
            this._onPropertyChanged('groupBy');
        }
    }

    get sortType() {
        return this._sortType;
    }
    set sortType(value) {
        if (this._sortType !== value) {
            this._sortType = value;
            this._onPropertyChanged('sortType');
        }
    }

    get sortOrder() {
        return this._sortOrder;
    }
    set sortOrder(value) {
        if (this._sortOrder !== value) {
            this._sortOrder = value;
            this._onPropertyChanged('sortOrder');
        }
    }

    get filters() {
        return this._filters.slice(0);
    }
    set filters(value) {
        if (this._filters !== value) {
            this._filters = value || [];
            this._onPropertyChanged('filters');
        }
    }
    getFilter(index) {
        return this._filters[index];
    }
    setFilter(index, filter) {
        let filters = this._filters;
        if (index >= 0 && filters[index] !== filter) {
            filters[index] = filter;
            this._onPropertyChanged('filters');
        }
    }
    addFilter(filter) {
        this._filters.push(filter);
        this._onPropertyChanged('filters');
    }
    removeFilter(filter) {
        let index = this._filters.indexOf(filter);
        if (index >= 0) {
            this.removeFilterAt(index);
        }
    }
    removeFilterAt(index) {
        let filters = this._filters;
        if (0 <= index && index < filters.length) {
            filters.splice(index, 1);
            this._onPropertyChanged('filters');
        }
    }
    clearFilters() {
        this._filters.splice(0);
        this._onPropertyChanged('filters');
    }

    _onPropertyChanged(name) {
        this.fire(Events.PropertyChanged, {
            field: this,
            name: name
        });
    }

    toJSON() {
        return {
            column: this._column,
            //columnType: this._columnType,
            alias: this._alias,
            table: this._table,
            output: this._output,
            groupBy: this._groupBy,
            sortType: this._sortType,
            sortOrder: this._sortOrder,
            filters: this._filters
        };
    }
    fromJSON(json) {
        if (!json) {
            return;
        }

        if (json.column) {
            this._column = json.column;
        }
        //if (json.columnType) {
        //    this._columnType = json.columnType;
        //}
        if (json.alias) {
            this._alias = json.alias;
        }
        if (json.table) {
            this._table = json.table;
        }
        if (json.output !== undefined) {
            this._output = json.output;
        }
        if (json.groupBy) {
            this._groupBy = json.groupBy;
        }
        if (json.sortType) {
            this._sortType = json.sortType;
        }
        if (json.sortOrder) {
            this._sortOrder = json.sortOrder;
        }
        if (json.filters && json.filters.length > 0) {
            this._filters = json.filters;
        }
    }
}

class QueryBuilder extends Event {
    constructor(fields) {
        super();

        this._fields = fields || [];
        this._distinct = false;

        this._groupBy = true;

        this._top = 0;
        this._limit = 0;
        this._offset = 0;

        this._joins = null;
        this._sql = ''; //generated sql

        this._quotePrefix = '[';
        this._quoteSuffix = ']';
        this._newLine = '\r\n';
    }

    get fields() {
        return this._fields.slice(0);
    }
    set fields(value) {
        this._fields = value || [];

        this._onFieldsChanged({
            changeType: ArrayChangedType.Reset,
            fields: this._fields
        });
    }

    get fieldFilterCount() {
        let count = 0;

        let fields = this._fields;
        for (var i = 0; i < fields.length; i++) {
            count = Math.max(count, fields[i].filters.length);
        }
        return count;
    }
    set fieldFilterCount(value) {
        if (value < 0) {
            return;
        }

        let fields = this._fields;
        for (var i = 0; i < fields.length; i++) {
            let field = fields[i];
            if (field.filters.length !== value) {
                field.filters = field.filters.slice(0, value);
            }
        }
    }

    getField(index) {
        let fields = this._fields;
        if (0 <= index && index < fields.length) {
            return fields[index];
        }
    }
    setField(index, field) {
        let fields = this._fields;
        if (0 <= index && index < fields.length) {
            fields[index] = field;

            this._onFieldsChanged({
                changeType: ArrayChangedType.ItemAdded,
                fields: [field]
            });
        }
    }
    addField(field) {
        if (!field) {
            return;
        }

        field.on(Events.PropertyChanged, this._onFieldChanged, this);
        this._fields.push(field);

        this._onFieldsChanged({
            changeType: ArrayChangedType.ItemAdded,
            fields: [field]
        });
    }
    removeField(field) {
        let fields = this._fields;

        this.removeFieldAt(fields.indexOf(field));
    }
    removeFieldAt(index) {
        let fields = this._fields;

        if (0 <= index && index < fields.length) {
            let removedFields = fields.splice(index, 1)[0];

            this._onFieldsChanged({
                changeType: ArrayChangedType.ItemDeleted,
                fields: removedFields
            });
        }
    }
    clearFields() {
        let removedFields = this._fields.splice(0);

        this._onFieldsChanged({
            changeType: ArrayChangedType.ItemDeleted,
            fields: removedFields
        });
    }

    get groupBy() {
        return this._groupBy;
    }
    set groupBy(value) {
        if (this._groupBy !== value) {
            this._groupBy = value;
            this._onSqlChanged();
        }
    }

    get newLine() {
        return this._newLine;
    }
    set newLine(value) {
        if (this._newLine !== value) {
            this._newLine = value;
            this._onSqlChanged();
        }
    }
    get quotePrefix() {
        return this._quotePrefix;
    }
    set quotePrefix(value) {
        if (this._quotePrefix !== value) {
            this._quotePrefix = value;
            this._onSqlChanged();
        }
    }
    get quoteSuffix() {
        return this._quoteSuffix;
    }
    set quoteSuffix(value) {
        if (this._quoteSuffix !== value) {
            this._quoteSuffix = value;
            this._onSqlChanged();
        }
    }

    get distinct() {
        return this._distinct;
    }
    set distinct(value) {
        if (this._distinct !== value) {
            this._distinct = value;
            this._onSqlChanged();
        }
    }

    get top() {
        return this._sql;
    }
    set top(value) {
        if (this._top !== value) {
            this._top = value;

            this._limit = 0;
            this._offset = 0;
            this._onSqlChanged();
        }
    }
    get limit() {
        return this._limit;
    }
    set limit(value) {
        if (this._limit !== value) {
            this._limit = value;

            this._top = 0;
            this._onSqlChanged();
        }
    }
    get offset() {
        return this._offset;
    }
    set offset(value) {
        if (this._offset !== value) {
            this._offset = value;

            this._top = 0;
            this._onSqlChanged();
        }
    }

    get joins() {
        return this._joins;
    }
    set joins(value) {
        if (this._joins !== value) {
            this._joins = value;
            this._onSqlChanged();
        }
    }

    get sql() {
        if (!this._sql) {
            this._sql = this._buildSql();
        }
        return this._sql;
    }

    _buildSql() {
        let fields = this._fields;
        if (fields.length === 0) {
            return '';
        }

        let sql = 'SELECT' + SPACE;

        // select
        if (this.distinct) {
            sql += 'DISTINCT' + SPACE;
        }
        if (this.top > 0) {
            sql += 'TOP' + SPACE;
        }
        sql += this._buildSelect();

        // from
        sql += (this.newLine + 'FROM' + SPACE + this._buildFrom());

        // where
        let where = this._buildWhere();
        if (where) {
            sql += (this.newLine + 'WHERE' + SPACE + where);
        }

        if (this.groupBy) {
            //group by
            let groupBy = this._buildGroupBy();
            if (groupBy) {
                sql += (this.newLine + 'GROUP BY' + SPACE + groupBy);
            }

            //having
            let having = this._buildHaving();
            if (having) {
                sql += (this.newLine + 'HAVING' + SPACE + having);
            }
        }

        // order by
        let orderBy = this._buildOrderBy();
        if (orderBy) {
            sql += (this.newLine + 'ORDER BY' + SPACE + orderBy);
        }

        // limit
        if (this.limit !== 0) {
            sql += (this.newLine + 'LIMIT' + SPACE + (this.offset > 0 ? this.offset + ',' + this.limit : this.limit));
        }

        // done
        sql += ';';

        return sql;
    }
    _buildSelect() {
        let select = '';

        let fields = this._fields;
        for (let i = 0; i < fields.length; i++) {
            let field = fields[i];
            if (field.output) {
                if (select !== '') {
                    select += (',' + SPACE);
                }
                select += this._getFieldName(field);

                if (field.alias) {
                    select += (SPACE + 'AS' + SPACE + this._bracketName(field.alias));
                }
            }
        }

        return select;
    }
    _buildFrom() {
        let from = '';

        let tables = [];
        let fields = this._fields;
        for (let i = 0; i < fields.length; i++) {
            let field = fields[i];

            if (tables.indexOf(field.table) === -1) {
                tables.push(field.table);
            }
        }

        let joinTables = [];
        let joins = this._joins;
        if (joins && joins.length > 0) {
            let leftTable = joins[0].leftTable;
            from += (leftTable + SPACE);
            joinTables.push(leftTable);

            for (let i = 0; i < joins.length; i++) {
                let relation = joins[i]; //{joinType: leftTable, rightTable, conditions:[]}
                let joinType = relation.joinType;
                let rightTable = relation.rightTable;
                let conditions = relation.conditions;

                let typeText = '';
                switch (joinType) {
                    case JoinType.Inner:
                        typeText = 'INNER JOIN';
                        break;
                    case JoinType.Left:
                        typeText = 'LEFT JOIN';
                        break;
                    case JoinType.Right:
                        typeText = 'RIGHT JOIN';
                        break;
                    case JoinType.Full:
                        typeText = 'FULL OUTER JOIN';
                        break;
                    default:
                        typeText = joinType;
                }
                from += (typeText + SPACE + rightTable);

                if (conditions && conditions.length > 0) {
                    from += (SPACE + 'ON' + SPACE + conditions.join(SPACE + 'AND' + SPACE));
                }

                joinTables.push(rightTable);
            }
        }

        //
        let hasMissing = joinTables.length < tables.length;
        if (hasMissing) {
            for (let i = 0; i < tables.length; i++) {
                let table = tables[i];

                if (joinTables.some(t => { return t.toLowerCase() === table.toLowerCase(); })) {
                    continue;
                }

                if (from !== '') {
                    from += (',' + SPACE);
                }
                from += this._bracketName(table);
            }
        }

        return from;
    }
    _buildGroupBy() {
        let groupBy = '';

        let fields = this._fields;
        for (let i = 0; i < fields.length; i++) {
            let field = fields[i];
            if (field.groupBy === GroupBy.GroupBy) {
                if (groupBy !== '') {
                    groupBy += ',' + SPACE;
                }

                groupBy += this._getFieldName(field);
            }
        }

        return groupBy;
    }
    _buildHaving() {
        let havingFields = this._fields.filter(f => {
            return (f.groupBy !== GroupBy.Where && !!f.filters.join(''));
        });

        return this._buildFilters(havingFields);
    }
    _buildWhere() {
        let fields = this._fields;
        let allExpression = !fields.some(f => {
            return f.groupBy !== GroupBy.Expression;
        });
        let whereFields = fields.filter(f => {
            return ((f.groupBy === GroupBy.Where || allExpression) && !!f.filters.join(''));
        });

        return this._buildFilters(whereFields);
    }
    _buildFilters(fields) {
        if (fields.length === 0) {
            return '';
        }

        let condition = '';
        let ffCount = this.fieldFilterCount;
        for (let i = 0; i < ffCount; i++) {
            let ands = '';

            for (let j = 0; j < fields.length; j++) {
                let filterExpr = this._getFilterExpr(fields[j], i);
                if (filterExpr) {
                    if (ands !== '') {
                        ands += (SPACE + 'AND' + SPACE);
                    }
                    ands += ('(' + filterExpr + ')');
                }
            }

            if (ands) {
                if (condition !== '') {
                    condition += (SPACE + 'OR' + this.newLine + TAB);
                }
                condition += ands;
            }
        }

        return condition;
    }
    _buildOrderBy() {
        let orderBy = '';

        let sorts = [];
        let fields = this._fields;
        for (let i = 0; i < fields.length; i++) {
            let field = fields[i];
            if (field.sortType !== SortType.Unsorted) {
                sorts.push(field);
            }
        }

        sorts.sort((a, b) => { return a.sortOrder - b.sortOrder; });

        for (let i = 0; i < sorts.length; i++) {
            let field = sorts[i];
            if (orderBy !== '') {
                orderBy += (',' + SPACE);
            }

            orderBy += (field.alias ? field.alias : this._getFieldName(field));

            if (field.sortType === SortType.Descending) {
                orderBy += ' DESC';
            }
        }

        return orderBy;
    }

    _getFieldName(field) {
        let name = '';

        if (field.table) {
            name = this._bracketName(field.table) + '.' + this._bracketName(field.column);
        } else {
            name = this._bracketName(field.column);
        }

        switch (field.groupBy) {
            case GroupBy.Sum: name = 'SUM(' + name + ')'; break;
            case GroupBy.Avg: name = 'AVG(' + name + ')'; break;
            case GroupBy.Min: name = 'MIN(' + name + ')'; break;
            case GroupBy.Max: name = 'MAX(' + name + ')'; break;
            case GroupBy.Count: name = 'COUNT(' + name + ')'; break;
            case GroupBy.SumDistinct: name = 'SUM(DISTINCT' + SPACE + name + ')'; break;
            case GroupBy.AvgDistinct: name = 'AVG(DISTINCT' + SPACE + name + ')'; break;
            case GroupBy.MinDistinct: name = 'MIN(DISTINCT' + SPACE + name + ')'; break;
            case GroupBy.MaxDistinct: name = 'MAX(DISTINCT' + SPACE + name + ')'; break;
            case GroupBy.CountDistinct: name = 'COUNT(DISTINCT' + SPACE + name + ')'; break;
            default: break;
        }

        return name;
    }
    _bracketName(name) { //use brackets to contain spaces, reserved words, etc
        let prefix = this.quotePrefix;
        let suffix = this.quoteSuffix;

        if (!prefix || !suffix) {
            return name;
        }

        if (name.length > 1 && name[0] === prefix && name[name.length - 1] === suffix) {
            return name;
        }

        let isExpression = name === '*' || exprReg.test(name);
        if (isExpression) {
            return name;
        }

        return prefix + name + suffix;
    }
    _getFilterExpr(field, filterIndex) {
        let filter = field.filters[filterIndex];
        if (!filter) {
            return '';
        }

        let name = this._getFieldName(field);
        let m = filter.match(filterReg1);
        if (m) {
            return m[1] === ''
                ? name + SPACE + m.slice(2).join(SPACE)// > x 
                : m.slice(1).join(SPACE); // y > x
        }

        m = filter.match(filterReg2);
        if (m) {
            return m[1] === ''
                ? name + (SPACE + 'BETWEEN' + SPACE) + m[2] + (SPACE + 'AND' + SPACE) + m[3]  // between x and y
                : m[1] + (SPACE + 'BETWEEN' + SPACE) + m[2] + (SPACE + 'AND' + SPACE) + m[3]; // z between x and y
        }

        //
        return (name + SPACE + filter);
    }

    _onSqlChanged() {
        this._sql = '';
        this.fire(Events.SqlChanged);
    }
    _onFieldChanged(args) {
        let field = args.field;
        let propertyName = args.name;

        if (propertyName === 'output') {
            this._aliasField(field);
        }

        this._onFieldsChanged({
            changeType: ArrayChangedType.ItemChanged,
            fields: [field]
        });
    }
    _onFieldsChanged(args) {
        let changedType = args.changeType;
        let fields = args.fields;

        if (changedType === ArrayChangedType.ItemAdded) {
            for (let i = 0; i < fields.length; i++) {
                this._aliasField(fields[i]);
            }
        }

        this._onSqlChanged();
    }
    _aliasField(field) {
        if (!field.alias && field.output) {
            let needAddAlias = this._fields.filter(f => {
                return (f !== field && !f.alias && f.output && f.column === field.column);
            }).length > 0;

            if (needAddAlias) {
                field.alias = this._getUniqueAlias(field);
            }
        }
    }
    _getUniqueAlias(field) {
        let index = 0;
        while (index++) {
            let alias = 'Expr' + index;

            let duplicate = false;
            let fields = this._fields;
            for (let i = 0; i < fields.length; i++) {
                if (fields[i] != field && alias.toLowerCase() === fields[i].toLowerCase()) {
                    duplicate = true;
                    break;
                }
            }

            //
            if (!duplicate) {
                return alias;
            }
        }
    }
}

export { SortType, GroupBy, QueryField, QueryBuilder };