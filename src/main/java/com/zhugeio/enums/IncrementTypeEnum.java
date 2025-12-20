package com.zhugeio.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum IncrementTypeEnum implements IEnum {

	ADD("增量"),
	ADD_ID("增量id类型"),
	ADD_TIME("增量time类型"),
	ALL("全量");
	
	private String desc;

	public static IncrementTypeEnum fromName(String name) {
		try {
			return name != null ? IncrementTypeEnum.valueOf(name.toUpperCase()) : null;
		} catch (IllegalArgumentException e) {
			return null;
		}
	}
}
