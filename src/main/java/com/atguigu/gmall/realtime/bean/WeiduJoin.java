package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author sunzhipeng
 * @create 2022-04-22 22:53
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WeiduJoin {
    private String page_id;
    private String display_type;
    private String item;
    private String item_type;
    private String pos_id;
    private String order;
}
