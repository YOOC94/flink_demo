package com.yc.flink.model;

import lombok.*;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class StudentEvent {

    private String name;

    private Long timestamp;

    private String sex;

    private Integer age;

}
