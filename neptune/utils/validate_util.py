from api.common.exceptions import ValidateException


def check_params_with_model(params, model, keep_extra=True):
    """
    参数校验逻辑
    :param params: {"name": "zhangsan", "score": 95, "info": {}}
    :param model: {"name": {"type": str, "required": True, "notnull": True, "format": {"in": ["", ""]}},
                  "score": {"type": str, "required": True, "notnull": True},
                  "info": {
                       "type": dict,
                       "notnull": True,
                       "fields": {
                                "parent_name": {"type": str, "required": True, "notnull": True},
                                "parent_country": {"type": str, "required": True, "notnull": True}
                            }
                       }
                  }
    :param keep_extra: 保留不再model中的额外参数
    :return:
    """
    for model_k, model_v in model.items():
        if "default" in model_v:
            params[model_k] = model_v.get("default")

    for model_k, model_v in model.items():
        if model_v.get("required"):
            if model_k not in params:
                raise ValidateException("缺少必填参数%s" % model_k)
        if model_v.get("notnull"):
            if model_k in params:
                if params.get(model_k) is None:
                    raise ValidateException("参数%s不能为空" % model_k)
        if model_v.get("type"):
            if model_k in params:
                if not isinstance(params.get(model_k), model_v.get("type")):
                    raise ValidateException("参数%s格式不正确，应为%s类型" % (model_k, model_v.get("type")))

        format_dict = model_v.get("format")
        if format_dict is not None:
            if not isinstance(format_dict, dict):
                raise ValidateException("format参数必须是dict")
            if model_k in params:
                if "in" in format_dict:
                    if params.get(model_k) not in format_dict.get("in"):
                        raise ValidateException("%s参数的值只能为%s" % (model_k, str(format_dict.get("in"))))
                if model_v.get("type") in [int, float]:
                    if ">" in format_dict:
                        if params.get(model_k) <= format_dict.get(">"):
                            raise ValidateException("%s参数的值必须大于%s" % (model_k, str(format_dict.get(">"))))
                    if "<" in format_dict:
                        if params.get(model_k) >= format_dict.get("<"):
                            raise ValidateException("%s参数的值必须小于%s" % (model_k, str(format_dict.get("<"))))
                    if ">=" in format_dict:
                        if params.get(model_k) < format_dict.get(">="):
                            raise ValidateException("%s参数的值必须大于等于%s" % (model_k, str(format_dict.get(">="))))
                    if "<=" in format_dict:
                        if params.get(model_k) > format_dict.get("<="):
                            raise ValidateException("%s参数的值必须小于等于%s" % (model_k, str(format_dict.get("<="))))
                    if "!=" in format_dict:
                        if params.get(model_k) == format_dict.get("!="):
                            raise ValidateException("%s参数的值不能等于%s" % (model_k, str(format_dict.get("!="))))

                if model_v.get("type") is str:
                    if "len" in format_dict:
                        len_dict = format_dict.get("len")
                        if ">" in len_dict:
                            if len(params.get(model_k)) <= len_dict.get(">"):
                                raise ValidateException("%s参数的长度必须大于%s" % (model_k, str(len_dict.get(">"))))
                        if "<" in len_dict:
                            if len(params.get(model_k)) >= len_dict.get("<"):
                                raise ValidateException("%s参数的长度必须小于%s" % (model_k, str(len_dict.get("<"))))
                        if ">=" in len_dict:
                            if len(params.get(model_k)) < len_dict.get(">="):
                                raise ValidateException("%s参数的长度必须大于等于%s" % (model_k, str(len_dict.get(">="))))
                        if "<=" in len_dict:
                            if len(params.get(model_k)) > len_dict.get("<="):
                                raise ValidateException("%s参数的长度必须小于等于%s" % (model_k, str(len_dict.get("<="))))

        fields = model_v.get("fields")
        inner_params = params.get(model_k)
        if fields is not None and inner_params is not None:
            if not model_v.get("type"):
                raise ValidateException("嵌套校验缺少类型type")
            if model_v.get("type") is dict:
                check_params_with_model(inner_params, fields)

    if keep_extra is False:
        resp_params = {}
        for model_k, model_v in model.items():
            if model_k in params:
                resp_params[model_k] = params.get(model_k)
            else:
                if "default" in model_v:
                    resp_params[model_k] = model_v.get("default")
        return resp_params
    return params


def pop_data_from_model(data, model):
    for model_k, model_v in model.items():
        if "need_pop" in model_v and model_v.get("need_pop") is True:
            data.pop(model_k, None)
    return data


if __name__ == "__main__":
    params = {
                "new_score": 1.6,
                "gender": "aaaa",
                "name": "lisi",
                "score": 199,
                "info": {"parent_name": "21",
                         "parent_country": {"inner_country": "fff"}}
            }

    model = {
                "new_score": {"type": float, "format": {">": 1.2}},
                "gender": {"type": str, "format": {"len": {">": 3, "<": 5}}},
                "name": {"type": str, "format": {"in": ["zhangsan", "lisi"], "len": {">": 3, "<=": 10}}},
                "score": {"type": int, "required": True, "notnull": True, "format": {">=": 100, "<=": 200}},
                "info": {
                     "type": dict,
                     "notnull": True,
                     "fields": {
                         "parent_name": {"type": str, "required": True, "notnull": True},
                         "parent_country": {"type": dict,
                                            "required": True,
                                            # "notnull": True,
                                            "fields": {
                                                "inner_country": {"type": str, "required": True, "notnull": True}
                                            }}
                        }
                    }
             }
    res = check_params_with_model(params, model)
    print(res)
