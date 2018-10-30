import json
import os
import time
import copy


root_path = "../4224-project-files/data-files"

warehouse_path = "warehouse.csv"
warehouse_attribute = ["w_id", "w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip",
                       "w_tax", "w_ytd"]
warehouse_primary_keys = ["w_id"]
warehouse_int_index = [0]
warehouse_float_index = [-1, -2]

district_path = "district.csv"
district_attribute = ["d_w_id", "d_id", "d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip",
                      "d_tax", "d_ytd", "d_next_o_id"]
district_primary_keys = ["d_w_id", "d_id"]
district_int_index = [0, 1, -1]
district_float_index = [-2, -3]

customer_path = "customer.csv"
customer_attribute = ["c_w_id", "c_d_id", "c_id", "c_first", "c_middle", "c_last",
                      "c_street_1", "c_street_2", "c_city", "c_state", "c_zip", "c_phone",
                      "c_since", "c_credit", "c_credit_lim", "c_discount", "c_balance",
                      "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt", "c_data"]
customer_primary_keys = ["c_w_id", "c_d_id", "c_id"]
customer_int_index = [0, 1, 2, -2, -3]
customer_float_index = [-4, -5, -6, -7]

item_path = "item.csv"
item_attribute = ["i_id", "i_name", "i_price", "i_im_id", "i_data"]
item_primary_keys = ["i_id"]
item_int_index = [0, 3]
item_float_index = [2]

order_path = "order.csv"
order_attribute = ["o_w_id", "o_d_id", "o_id", "o_c_id", "o_carrier_id", "o_ol_cnt",
                   "o_all_local", "o_entry_d"]
order_primary_keys = ["o_w_id", "o_d_id", "o_id"]
order_int_index = [0, 1, 2, 3, 4]
order_float_index = [-2, -3]

order_line_path = "order-line.csv"
order_line_attribute = ["ol_w_id", "ol_d_id", "ol_o_id", "ol_number", "ol_i_id", "ol_delivery_d", "ol_amount",
                        "ol_supply_w_id", "ol_quantity", "ol_dist_info"]
order_line_primary_keys = ["ol_w_id", "ol_d_id", "ol_o_id", "ol_number"]
order_line_int_index = [0, 1, 2, 3, 4, -3]
order_line_float_index = [-2, -4]

stock_path = "stock.csv"
stock_attribute = ["s_w_id", "s_i_id", "s_quantity", "s_ytd", "s_order_cnt", "s_remote_cnt",
                   "s_dist_01", "s_dist_02", "s_dist_03", "s_dist_04", "s_dist_05",
                   "s_dist_06", "s_dist_07", "s_dist_08", "s_dist_09", "s_dist_10", "s_data"]
stock_primary_keys = ["s_w_id", "s_i_id"]
stock_int_index = [0, 1, 4, 5]
stock_float_index = [2, 3]


def extract_feature(path, attributes, int_index, float_index):
    all_records = list()
    with open(path) as f:
        lines = [x.strip('\n').split(',') for x in f.readlines()]
    for row in lines:
        record = dict()
        if len(row) != len(attributes):
            raise AssertionError("attributes length mismatch")
        for index in int_index:
            if row[index] == "null":
                row[index] = -1
            row[index] = int(row[index])
        for index in float_index:
            row[index] = float(row[index])
        for i, att in enumerate(attributes):
            record[att] = row[i]
        all_records.append(record)
    return all_records


def vertical_split_attribute(original_records, split_attributes, primary_keys):
    split_records = list()
    for entry in original_records:
        split_entry = dict()
        for key in primary_keys:
            split_entry[key] = entry[key]
        for attribute in split_attributes:
            split_entry[attribute] = entry[attribute]
            del entry[attribute]
        split_records.append(split_entry)
    return original_records, split_records


def rename_attribute(all_records, old_attributes, new_attributes):
    if len(old_attributes) != len(new_attributes):
        raise AssertionError("predicate length mismatch")
    for record in all_records:
        for i in range(len(old_attributes)):
            record[new_attributes[i]] = record[old_attributes[i]]
            del record[old_attributes[i]]
    return all_records


def delete_attribute(all_records, attributes):
    for record in all_records:
        for attribute in attributes:
            del record[attribute]
    return all_records


def write_to_json(all_records, path):
    with open (path, "w") as fp:
        for record in all_records:
            json.dump(record, fp)
            fp.write("\n")
        # for entry in all_records:
        #     fp.write(str(entry) + "\n")


def check_predicate_matched(master, slave, master_predicates, slave_predicates):
    for i in range(len(master_predicates)):
        if master[master_predicates[i]] != slave[slave_predicates[i]]:
            return False
    return True


def flat_nest_loop_join(masters, slaves, master_predicates, slave_predicates):
    if len(master_predicates) != len(slave_predicates):
        raise AssertionError("predicate length mismatch")
    for master in masters:
        for slave in slaves:
            if not check_predicate_matched(master, slave, master_predicates, slave_predicates):
                continue
            for key in slave:
                if key in slave_predicates:
                    continue
                master[key] = slave[key]
    return masters


def flat_sort_merge_join(masters, slaves, master_predicates, slave_predicates):
    if len(master_predicates) != len(slave_predicates):
        raise AssertionError("predicate length mismatch")
    i = 0
    j = 0
    while i < len(masters) and j < len(slaves):
        master = masters[i]
        slave = slaves[j]
        if check_predicate_matched(master, slave, master_predicates, slave_predicates):
            for key in slave:
                if key in slave_predicates:
                    continue
                master[key] = slave[key]
            i += 1
        else:
            j += 1
    return masters


def nest_loop_join(masters, slaves, master_predicates, slave_predicates, field_name):
    if len(master_predicates) != len(slave_predicates):
        raise AssertionError("predicate length mismatch")
    for master in masters:
        master[field_name] = list()
        for slave in slaves:
            if not check_predicate_matched(master, slave, master_predicates, slave_predicates):
                continue
            slave_copy = copy.deepcopy(slave)
            for key in slave_predicates:
                del slave_copy[key]
            master[field_name].append(slave_copy)
    return masters


def sort_merge_join(masters, slaves, master_predicates, slave_predicates, field_name):
    if len(master_predicates) != len(slave_predicates):
        raise AssertionError("predicate length mismatch")
    for master in masters:
        master[field_name] = list()
    i = 0
    j = 0
    while i < len(masters) and j < len(slaves):
        master = masters[i]
        slave = slaves[j]
        if check_predicate_matched(master, slave, master_predicates, slave_predicates):
            slave_copy = copy.deepcopy(slave)
            for key in slave_predicates:
                del slave_copy[key]
            masters[i][field_name].append(slave_copy)
            j += 1
        else:
            i += 1
    return masters


def process_district_json():
    all_warehouse_records = extract_feature(os.path.join(root_path, warehouse_path), warehouse_attribute, warehouse_int_index, warehouse_float_index)
    warehouse_address_attribute = ["w_street_1", "w_street_2", "w_city", "w_state", "w_zip"]
    warehouse_1, warehouse_address = vertical_split_attribute(all_warehouse_records, warehouse_address_attribute, warehouse_primary_keys)
    warehouse_2, warehouse_ytd = vertical_split_attribute(warehouse_1, ["w_ytd"], warehouse_primary_keys)

    all_district_records = extract_feature(os.path.join(root_path, district_path), district_attribute, district_int_index, district_float_index)
    district_address_attribute = ["d_street_1", "d_street_2", "d_city", "d_state", "d_zip"]
    district_1, district_address = vertical_split_attribute(all_district_records, district_address_attribute, district_primary_keys)
    district_2, district_ytd = vertical_split_attribute(district_1, ["d_ytd"], district_primary_keys)
    district_3, district_next_o_id = vertical_split_attribute(district_2, ["d_next_o_id"], district_primary_keys)

    w_d_ytd = nest_loop_join(warehouse_ytd, district_ytd, warehouse_primary_keys, ["d_w_id"], "districts")
    w_d_address = rename_attribute(flat_nest_loop_join(district_address, warehouse_address, ["d_w_id"], ["w_id"]), ["d_w_id"], ["w_id"])
    write_to_json(district_next_o_id, "next_avail_order.json")
    write_to_json(w_d_address, "district_address.json")
    write_to_json(w_d_ytd, "district.json")
    print("District json finish")


def process_customer_json():
    all_warehouse_records = extract_feature(os.path.join(root_path, warehouse_path), warehouse_attribute, warehouse_int_index, warehouse_float_index)
    all_district_records = extract_feature(os.path.join(root_path, district_path), district_attribute, district_int_index, district_float_index)
    _, w_name_tax = vertical_split_attribute(all_warehouse_records, ["w_name", "w_tax"], warehouse_primary_keys)
    _, d_name_tax = vertical_split_attribute(all_district_records, ["d_name", "d_tax"], district_primary_keys)
    name_tax = flat_nest_loop_join(d_name_tax, w_name_tax, ["d_w_id"], warehouse_primary_keys)

    all_customer_records = extract_feature(os.path.join(root_path, customer_path), customer_attribute, customer_int_index, customer_float_index)
    customer_1, customer_misc = vertical_split_attribute(all_customer_records, ["c_data"], customer_primary_keys)

    name_tax = sorted(name_tax, key=lambda d: (d["d_w_id"], d["d_id"]))
    customer_1 = sorted(customer_1, key=lambda d: (d["c_w_id"], d["c_d_id"]))
    final_customers = flat_sort_merge_join(customer_1, name_tax, ["c_w_id", "c_d_id"], district_primary_keys)
    write_to_json(final_customers, "customer.json")
    write_to_json(customer_misc, "customer_misc.json")
    print("Customer json finish")


def process_order_json():
    all_order_records = extract_feature(os.path.join(root_path, order_path), order_attribute, order_int_index, order_float_index)
    all_order_line_records = extract_feature(os.path.join(root_path, order_line_path), order_line_attribute, order_line_int_index, order_line_float_index)
    all_order_records = sorted(all_order_records, key=lambda d: (d["o_w_id"], d["o_d_id"], d["o_c_id"]))
    all_order_line_records = sorted(all_order_line_records, key=lambda d: (d["ol_i_id"]))

    all_customer_records = extract_feature(os.path.join(root_path, customer_path), customer_attribute, customer_int_index, customer_float_index)
    _, customer_name = vertical_split_attribute(all_customer_records, ["c_first", "c_middle", "c_last"], customer_primary_keys)
    all_item_records = extract_feature(os.path.join(root_path, item_path), item_attribute, item_int_index, item_float_index)
    _, item_name = vertical_split_attribute(all_item_records, ["i_name"], item_primary_keys)
    customer_name = sorted(customer_name, key=lambda d: (d["c_w_id"], d["c_d_id"], d["c_id"]))
    item_name = sorted(item_name, key=lambda d: d["i_id"])
    order_line = flat_sort_merge_join(all_order_line_records, item_name, ["ol_i_id"], item_primary_keys)
    order = flat_sort_merge_join(all_order_records, customer_name, ["o_w_id", "o_d_id", "o_c_id"], customer_primary_keys)

    order = sorted(order, key=lambda d: (d["o_w_id"], d["o_d_id"], d["o_id"]))
    order_line = sorted(order_line, key=lambda d: (d["ol_w_id"], d["ol_d_id"], d["ol_o_id"]))
    order_with_order_line = sort_merge_join(order, order_line, ["o_w_id", "o_d_id", "o_id"],
                                            ["ol_w_id", "ol_d_id", "ol_o_id"], "order_lines")

    for order in order_with_order_line:
        delivery_d = order["order_lines"][0]["ol_delivery_d"]
        order["o_delivery_d"] = delivery_d
        order["order_lines"] = delete_attribute(order["order_lines"], ["ol_delivery_d"])
    write_to_json(order_with_order_line, "order.json")
    print("Order json finished")


def process_customer_order_json():
    all_customer_records = extract_feature(os.path.join(root_path, customer_path), customer_attribute, customer_int_index, customer_float_index)
    _, customer_id = vertical_split_attribute(all_customer_records, [], customer_primary_keys)
    all_order_records = extract_feature(os.path.join(root_path, order_path), order_attribute, order_int_index, order_float_index)
    all_order_line_records = extract_feature(os.path.join(root_path, order_line_path), order_line_attribute, order_line_int_index, order_line_float_index)

    _, order = vertical_split_attribute(all_order_records, ["o_c_id"], order_primary_keys)
    _, order_line = vertical_split_attribute(all_order_line_records, ["ol_i_id"], ["ol_w_id", "ol_d_id", "ol_o_id"])
    order = sorted(order, key=lambda d: (d["o_w_id"], d["o_d_id"], d["o_id"]))
    order_line = sorted(order_line, key=lambda d: (d["ol_w_id"], d["ol_d_id"], d["ol_o_id"]))
    order_with_order_line = sort_merge_join(order, order_line, ["o_w_id", "o_d_id", "o_id"],
                                            ["ol_w_id", "ol_d_id", "ol_o_id"], "order_lines")

    order_with_order_line = sorted(order_with_order_line, key=lambda d: (d["o_w_id"], d["o_d_id"], d["o_c_id"]))
    customer_id = sorted(customer_id, key=lambda d: (d["c_w_id"], d["c_d_id"], d["c_id"]))
    customer_order = sort_merge_join(customer_id, order_with_order_line, ["c_w_id", "c_d_id", "c_id"],
                                     ["o_w_id", "o_d_id", "o_c_id"], "orders")
    write_to_json(customer_order, "customer_order.json")
    print("Customer order json finished")


def process_item_stock_json():
    all_item_records = extract_feature(os.path.join(root_path, item_path), item_attribute, item_int_index, item_float_index)
    all_stock_records = extract_feature(os.path.join(root_path, stock_path), stock_attribute, stock_int_index, stock_float_index)
    stock_split_attributes = ["s_data"]
    for i in range(1, 11):
        stock_split_attributes.append("s_dist_" + str(i).zfill(2))
    stock, stock_misc = vertical_split_attribute(all_stock_records, stock_split_attributes, stock_primary_keys)
    item, item_misc = vertical_split_attribute(all_item_records, ["i_im_id", "i_data"], item_primary_keys)

    item = sorted(item, key=lambda d: d["i_id"])
    stock = sorted(stock, key=lambda d: d["s_i_id"])
    item_stock = sort_merge_join(item, stock, ["i_id"], ["s_i_id"], "stocks")
    write_to_json(item_misc, "item_misc.json")
    write_to_json(stock_misc, "stock_misc.json")
    write_to_json(item_stock, "item_stock.json")
    print("Item stock json finished")


start_time = time.time()
process_district_json()
process_customer_json()
process_order_json()
process_customer_order_json()
process_item_stock_json()
end_time = time.time()
print("All finished within: " + str(end_time - start_time) + " seconds")
